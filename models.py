"""
Pydantic models for Indian High Court Judgments data structures
"""

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from datetime import datetime


class IndexFile(BaseModel):
    """
    Model for index.json files (both data.index.json and metadata.index.json)

    These files track the contents of tar archives and provide metadata about
    the files stored in S3 for each court/bench combination.
    """

    files: List[str] = Field(
        default_factory=list,
        description="List of filenames contained in the tar archive",
    )
    file_count: int = Field(
        default=0, ge=0, description="Number of files in the tar archive"
    )
    tar_size: int = Field(
        default=0, ge=0, description="Size of the tar archive in bytes"
    )
    tar_size_human: str = Field(
        default="0 B",
        description="Human-readable size of the tar archive (e.g., '1.5 GB')",
    )
    updated_at: str = Field(
        description="ISO format datetime string of when the index was last updated"
    )

    @field_validator("file_count")
    @classmethod
    def validate_file_count_matches_files(cls, v, info):
        """Ensure file_count matches the actual number of files"""
        if "files" in info.data and len(info.data["files"]) != v:
            # Allow mismatch during construction, will be fixed when serializing
            pass
        return v

    @field_validator("updated_at")
    @classmethod
    def validate_datetime_format(cls, v):
        """Validate that updated_at is a valid ISO format datetime"""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            raise ValueError(
                f"updated_at must be a valid ISO format datetime string, got: {v}"
            )
        return v

    def model_post_init(self, __context):
        """Ensure file_count matches files list after initialization"""
        self.file_count = len(self.files)


class JudgmentMetadata(BaseModel):
    """
    Model for individual judgment metadata JSON files

    Each judgment downloaded from ecourts has an associated metadata file
    containing information extracted from the search results page.
    """

    court_code: str = Field(description="Court code in format like '27~1' or '33~10'")
    court_name: str = Field(
        description="Full name of the court (e.g., 'Bombay High Court')"
    )
    raw_html: str = Field(
        description="Raw HTML snippet from the search results containing judgment details"
    )
    pdf_link: str = Field(
        description="Relative path to the PDF file (e.g., 'court/cnrorders/hcbgoa/orders/...')"
    )
    downloaded: bool = Field(
        default=False, description="Whether the PDF has been successfully downloaded"
    )

    @field_validator("court_code")
    @classmethod
    def validate_court_code_format(cls, v):
        """Validate court code format"""
        if "~" not in v:
            raise ValueError(f"court_code must contain '~' separator, got: {v}")
        parts = v.split("~")
        if len(parts) != 2:
            raise ValueError(
                f"court_code must have exactly two parts separated by '~', got: {v}"
            )
        try:
            int(parts[0])
            int(parts[1])
        except ValueError:
            raise ValueError(f"court_code parts must be numeric, got: {v}")
        return v

    @field_validator("pdf_link")
    @classmethod
    def validate_pdf_link(cls, v):
        """Validate pdf_link format"""
        if not v.startswith("court/"):
            raise ValueError(f"pdf_link must start with 'court/', got: {v}")
        if not v.endswith(".pdf"):
            raise ValueError(f"pdf_link must end with '.pdf', got: {v}")
        return v


class S3UploadResult(BaseModel):
    """
    Model for tracking S3 upload results by bench
    """

    bench_name: str = Field(description="Name of the bench")
    upload_success: bool = Field(description="Whether the upload was successful")
    metadata_count: int = Field(
        default=0, ge=0, description="Number of metadata files uploaded"
    )
    data_count: int = Field(
        default=0, ge=0, description="Number of data files (PDFs) uploaded"
    )
    error_message: Optional[str] = Field(
        default=None, description="Error message if upload failed"
    )


class CourtBenchDates(BaseModel):
    """
    Model for tracking latest dates for court benches from S3 index files
    """

    court_code: str = Field(description="Court code in S3 format (e.g., '27_1')")
    benches: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of bench names to their latest updated_at timestamps",
    )

    @field_validator("court_code")
    @classmethod
    def validate_s3_court_code_format(cls, v):
        """Validate S3 court code format (uses underscore)"""
        if "_" not in v:
            raise ValueError(f"S3 court_code must contain '_' separator, got: {v}")
        parts = v.split("_")
        if len(parts) != 2:
            raise ValueError(
                f"S3 court_code must have exactly two parts separated by '_', got: {v}"
            )
        try:
            int(parts[0])
            int(parts[1])
        except ValueError:
            raise ValueError(f"S3 court_code parts must be numeric, got: {v}")
        return v

    def get_earliest_date(self) -> Optional[datetime]:
        """Get the earliest date across all benches"""
        if not self.benches:
            return None

        dates = []
        for date_str in self.benches.values():
            try:
                date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                dates.append(date_obj)
            except (ValueError, AttributeError):
                continue

        return min(dates) if dates else None
