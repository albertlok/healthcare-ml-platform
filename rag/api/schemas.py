"""Pydantic request/response models for the RAG inference API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=5, max_length=500, description="Natural language query")
    top_k: int = Field(default=5, ge=1, le=20, description="Number of documents to retrieve")
    filter_patient_id: str | None = Field(default=None, description="Restrict retrieval to a specific patient")


class RetrievedDocument(BaseModel):
    id: str
    text: str
    score: float
    patient_id: str | None
    chief_complaint: str | None
    no_show_risk: float | None


class QueryResponse(BaseModel):
    query: str
    answer: str
    retrieved_docs: list[RetrievedDocument]
    model_used: str
    latency_ms: float


class PatientRiskRequest(BaseModel):
    patient_id: str
    appointment_id: str | None = None


class PatientRiskResponse(BaseModel):
    patient_id: str
    no_show_probability: float
    risk_tier: str                 # LOW / MEDIUM / HIGH
    top_risk_factors: list[str]
    recommendation: str
    supporting_notes_summary: str  # RAG-generated from clinical notes
    model_version: str


class EmbedRequest(BaseModel):
    patient_id: str
    appointment_id: str
    note_text: str = Field(..., min_length=20, max_length=5000)


class EmbedResponse(BaseModel):
    document_id: str
    patient_id: str
    status: str


class HealthResponse(BaseModel):
    status: str
    chromadb: str
    embedding_model: str
    collection_count: int
