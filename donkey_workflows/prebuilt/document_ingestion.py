from donkey.core.base.schema import TransformerComponent
from donkey.core.document import Document
from donkey.core.loaders import BaseLoader
from donkey.core.vector_stores import BaseVectorStore
from donkey_toolkit import validate_enum
from pydantic import BaseModel, Field

from donkey_workflows.context import Context
from donkey_workflows.decorators import step
from donkey_workflows.events import Event, StartEvent, StopEvent
from donkey_workflows.workflow import Workflow

# ============================================================================
# Enums
# ============================================================================


class DocStrategy:
    """
    Document de-duplication strategies work by comparing the hashes in the vector store.
    They require a vector store to be set.
    """

    DEDUPLICATE_OFF = "deduplicate_off"
    DUPLICATE_ONLY = "duplicate_only"
    DUPLICATE_AND_DELETE = "duplicate_and_delete"


# ============================================================================
# Events
# ============================================================================


class DocumentsLoadedEvent(Event):
    """Event when documents are loaded."""

    documents: list[Document] = Field(
        default_factory=list, description="Loaded documents"
    )


class DocumentsDeduplicatedEvent(Event):
    """Event after deduplication."""

    documents: list[Document] = Field(
        default_factory=list, description="Deduplicated documents"
    )


class DocumentsTransformedEvent(Event):
    """Event after transformation."""

    documents: list[Document] = Field(
        default_factory=list, description="Transformed documents"
    )


# ============================================================================
# Workflow State
# ============================================================================


class WorkflowState(BaseModel):
    """State for document ingestion workflow."""

    input_documents: list[Document] = Field(
        default_factory=list, description="Input documents"
    )
    processed_documents: list[Document] = Field(
        default_factory=list, description="Final processed documents"
    )


# ============================================================================
# Document Ingestion Workflow
# ============================================================================


class DocumentIngestionWorkflow(Workflow):
    """
    A document ingestion workflow for processing and storing documents.

    This workflow orchestrates the document ingestion pipeline with support for:
    - Multiple document loaders (e.g., Docx, PDF, S3)
    - Multiple transformation components (e.g., chunking, embedding)
    - Deduplication strategies
    - Vector store integration

    Attributes:
        transformers (list[TransformerComponent]): A list of transformer components applied to the input documents.
        doc_strategy (str, optional): The strategy used for handling document duplicates. (default: "duplicate_only")
        post_transformer (bool, optional): Whether document de-duplication should be applied after transformations
        loaders (list[BaseLoader]): List of loaders for loading or fetching documents.
        vector_store (BaseVectorStore): Vector store for storing processed documents.

    Example:
        ```python
        from donkey_workflows.prebuilt import DocumentIngestionWorkflow

        from donkey.core.text_chunkers import TokenTextChunker
        from donkey.embeddings.huggingface import HuggingFaceEmbedding


        ingestion_workflow = DocumentIngestionWorkflow(
            transformers=[
                TokenTextChunker(),
                HuggingFaceEmbedding(model_name="intfloat/multilingual-e5-small"),
            ]
        )

        result = await ingestion_workflow.run(documents=[doc1, doc2])
        ```
    """

    def __init__(
        self,
        transformers: list[TransformerComponent],
        doc_strategy: str = DocStrategy.DUPLICATE_ONLY,
        post_transformer: bool = False,
        loaders: list[BaseLoader] | None = None,
        vector_store: BaseVectorStore | None = None,
    ) -> None:
        validate_enum(
            el=doc_strategy, el_name="doc_strategy", expected_enum=DocStrategy
        )

        self.doc_strategy = doc_strategy
        self.transformers = transformers
        self.post_transformer = post_transformer
        self.loaders = loaders or []
        self.vector_store = vector_store

    @step(when=StartEvent)
    async def load_documents(
        self, ctx: Context[WorkflowState], ev: StartEvent
    ) -> DocumentsLoadedEvent:
        """
        Load documents.

        This step collects documents from all configured loaders and
        any documents passed directly to the workflow.
        """
        input_documents = []

        documents = ev.get("documents", [])
        if documents:
            input_documents.extend(documents)

        for loader in self.loaders:
            input_documents.extend(loader.load_data())

        async with ctx.store.edit_state() as state:
            state.input_documents = input_documents

        return DocumentsLoadedEvent(documents=input_documents)

    @step(when=DocumentsLoadedEvent)
    async def deduplicate_before_transform(
        self, ctx: Context[WorkflowState], ev: DocumentsLoadedEvent
    ) -> DocumentsDeduplicatedEvent | DocumentsTransformedEvent:
        """
        Apply deduplication before transformation (parent level) if configured.

        This step runs only if:
        - Vector store is configured
        - Deduplication is enabled
        - post_transformer is False
        """
        documents = ev.documents

        if (
            self.vector_store is not None
            and self.doc_strategy != DocStrategy.DEDUPLICATE_OFF
            and not self.post_transformer
        ):
            # Apply deduplication at parent level
            documents = self._handle_duplicates(documents)
            return DocumentsDeduplicatedEvent(documents=documents)
        else:
            # Skip deduplication, go directly to transformation
            return DocumentsTransformedEvent(documents=documents)

    @step(when=DocumentsDeduplicatedEvent)
    async def transform_documents(
        self, ctx: Context[WorkflowState], ev: DocumentsDeduplicatedEvent
    ) -> DocumentsTransformedEvent:
        """Applies all configured transformers in sequence."""
        documents = ev.documents

        if documents:
            documents = self._run_transformers(documents, self.transformers)

        return DocumentsTransformedEvent(documents=documents)

    @step(when=DocumentsTransformedEvent)
    async def save_documents(
        self, ctx: Context[WorkflowState], ev: DocumentsTransformedEvent
    ) -> StopEvent:
        """
        Finalize document processing.

        This step:
        - Applies post-transformation deduplication if configured
        - Saves documents to vector store if configured
        """
        documents = ev.documents

        # Apply deduplication after transformation (chunk level)
        if (
            self.vector_store is not None
            and self.doc_strategy != DocStrategy.DEDUPLICATE_OFF
            and self.post_transformer
            and documents
        ):
            documents = self._handle_duplicates(documents)

        # Save to vector store
        if self.vector_store is not None and documents:
            self.vector_store.add_documents(documents)

        async with ctx.store.edit_state() as state:
            state.processed_documents = documents

        return StopEvent(result=documents)

    def _handle_duplicates(self, documents: list[Document]) -> list[Document]:
        if self.vector_store is None:
            return documents

        ids, existing_hashes, existing_ref_hashes = (
            self.vector_store.get_all_document_hashes()
        )

        if self.post_transformer:
            # Use document hash (chunks level) for de-duplication
            hashes_fallback = existing_hashes
        else:
            # Use parent document hash `ref_doc_hash` (parent level)
            # Fallback to document hash if `ref_doc_hash` is missing for de-duplication
            hashes_fallback = [
                existing_ref_hashes[i]
                if existing_ref_hashes[i] is not None
                else existing_hashes[i]
                for i in range(len(existing_ref_hashes))
            ]

        current_hashes = []
        current_unique_hashes = []
        dedup_documents_to_run = []

        for doc in documents:
            current_hashes.append(doc.hash)

            if (
                doc.hash not in hashes_fallback
                and doc.hash not in current_unique_hashes
                and doc.get_content() != ""
            ):
                dedup_documents_to_run.append(doc)
                # Prevent duplicating same document hash in same batch
                current_unique_hashes.append(doc.hash)

        # Handle DUPLICATE_AND_DELETE strategy
        if self.doc_strategy == DocStrategy.DUPLICATE_AND_DELETE:
            ids_to_remove = [
                ids[i]
                for i in range(len(hashes_fallback))
                if hashes_fallback[i] not in current_hashes
            ]

            if ids_to_remove:
                self.vector_store.delete_documents(ids_to_remove)

        return dedup_documents_to_run

    def _run_transformers(
        self,
        documents: list[Document],
        transformers: list[TransformerComponent],
    ) -> list[Document]:
        _documents = documents.copy()

        for transformer in transformers:
            _documents = transformer(_documents)

        return _documents
