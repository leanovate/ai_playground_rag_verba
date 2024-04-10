import os
from datetime import datetime

import dotenv
import markdownify
from atlassian import Confluence
from wasabi import msg

from goldenverba.components.reader.document import Document
from goldenverba.components.reader.interface import InputForm, Reader


class ConfluenceReader(Reader):
    """
    The ConfluenceReader downloads files from Confluence and ingests them into Weaviate.
    """

    def __init__(self):
        super().__init__()
        self.name = "ConfluenceReader"
        self.requires_env = ["CONFLUENCE_API_KEY",
                             "CONFLUENCE_URL",
                             "CONFLUENCE_USER_EMAIL"]
        self.description = "Downloads all pages from the given confluence space and ingests it into Verba."
        self.input_form = InputForm.INPUT.value

    def load(
            self,
            bytes: list[str] = None,
            contents: list[str] = None,
            paths: list[str] = None,
            fileNames: list[str] = None,
            document_type: str = "Wiki",
    ) -> list[Document]:
        """Ingest data into Weaviate
        @parameter: bytes : list[str] - List of bytes
        @parameter: contents : list[str] - List of string content
        @parameter: paths : list[str] - List of paths to files
        @parameter: fileNames : list[str] - List of file names
        @parameter: document_type : str - Document type
        @returns list[Document] - Lists of documents.
        """
        if paths is None:
            paths = []
        documents = []

        api_token = os.environ['CONFLUENCE_API_KEY']
        user_email = os.environ['CONFLUENCE_USER_EMAIL']
        url = os.environ['CONFLUENCE_URL']

        confluence = Confluence(url=url,
                                username=user_email,
                                password=api_token)

        for space in paths:
            pages_meta = confluence.get_all_pages_from_space(space)
            for page_meta in pages_meta:
                msg.good(f"Loading page {page_meta['id']}, space {space}")
                page = confluence.get_page_by_id(page_meta['id'], "space,body.view,version,container")
                msg.good(f"Creating Document")
                document = Document(
                    text=markdownify.markdownify(page['body']['view']['value']),
                    type=document_type,
                    name=page['title'],
                    link=url + page['_links']['webui'],
                    path=page['space']['name'],
                    timestamp=str(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ),
                    reader=self.name,
                )
                documents.append(document)

        msg.good(f"Loaded {len(documents)} documents")
        return documents
