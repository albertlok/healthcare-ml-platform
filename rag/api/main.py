"""
RAG Inference API — FastAPI application.

Endpoints:
  GET  /health                        Health check
  POST /query                         Free-text RAG query over clinical notes
  GET  /patient/{patient_id}/risk     No-show risk summary with RAG context
  POST /embed                         Upsert a new clinical note

Start:
  uvicorn rag.api.main:app --reload --host 0.0.0.0 --port 8888

Production equivalent:
  - AWS: Lambda + API Gateway (or ECS Fargate)
  - Azure: Azure Container Apps
  - GCP: Cloud Run
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import chromadb
import structlog
import xgboost as xgb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sentence_transformers import SentenceTransformer

from rag.api.schemas import (
    EmbedRequest,
    EmbedResponse,
    HealthResponse,
    PatientRiskResponse,
    QueryRequest,
    QueryResponse,
    RetrievedDocument,
)

load_dotenv()

log = structlog.get_logger()

CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
COLLECTION_NAME = os.getenv("CHROMA_COLLECTION_CLINICAL_NOTES", "clinical_notes")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
MODEL_PATH = Path("models/no_show_model.json")
FEAT_NAMES_PATH = Path("models/feature_names.json")
CLASSIFICATION_THRESHOLD = float(os.getenv("CLASSIFICATION_THRESHOLD", "0.35"))

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Healthcare ML RAG API",
    description="Patient no-show risk scoring with RAG-powered clinical context.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Lazy-loaded singletons ────────────────────────────────────────────────────
_chroma_client: chromadb.HttpClient | None = None
_collection: chromadb.Collection | None = None
_embed_model: SentenceTransformer | None = None
_booster: xgb.Booster | None = None
_feature_names: list[str] | None = None


def get_chroma() -> chromadb.Collection:
    global _chroma_client, _collection
    if _collection is None:
        _chroma_client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        _collection = _chroma_client.get_or_create_collection(COLLECTION_NAME)
    return _collection


def get_embed_model() -> SentenceTransformer:
    global _embed_model
    if _embed_model is None:
        log.info("loading_embedding_model", model=EMBEDDING_MODEL)
        _embed_model = SentenceTransformer(EMBEDDING_MODEL)
    return _embed_model


def get_booster() -> tuple[xgb.Booster, list[str]] | tuple[None, None]:
    global _booster, _feature_names
    if _booster is None and MODEL_PATH.exists():
        _booster = xgb.Booster()
        _booster.load_model(str(MODEL_PATH))
        _feature_names = json.loads(FEAT_NAMES_PATH.read_text())
    return _booster, _feature_names


def _build_answer(query: str, docs: list[dict]) -> str:
    """Generate a concise answer from retrieved documents.

    Attempts to use Claude via Anthropic SDK if ANTHROPIC_API_KEY is set.
    Falls back to a rule-based summary if no API key is available — ensuring
    this works completely offline with zero cost.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")

    context = "\n\n".join([f"Note {i+1}: {d['document']}" for i, d in enumerate(docs)])

    if api_key:
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=api_key)
            message = client.messages.create(
                model=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
                max_tokens=400,
                system=(
                    "You are a clinical data assistant. Answer the question using only "
                    "the provided clinical note excerpts. Be concise (2-3 sentences). "
                    "Do not invent information not present in the notes."
                ),
                messages=[{"role": "user", "content": f"Context:\n{context}\n\nQuestion: {query}"}],
            )
            return message.content[0].text
        except Exception as e:
            log.warning("llm_call_failed_using_fallback", error=str(e))

    # Offline fallback: extract key sentences from top doc
    if docs:
        top_doc = docs[0]["document"]
        sentences = top_doc.split(". ")
        return ". ".join(sentences[:3]) + "." if sentences else top_doc
    return "No relevant clinical notes found for this query."


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    try:
        collection = get_chroma()
        count = collection.count()
        chroma_status = "ok"
    except Exception as e:
        chroma_status = f"error: {e}"
        count = 0

    return HealthResponse(
        status="ok",
        chromadb=chroma_status,
        embedding_model=EMBEDDING_MODEL,
        collection_count=count,
    )


@app.post("/query", response_model=QueryResponse)
def query(request: QueryRequest) -> QueryResponse:
    t0 = time.perf_counter()

    model = get_embed_model()
    collection = get_chroma()

    query_embedding = model.encode(request.query).tolist()

    where_filter = (
        {"patient_id": {"$eq": request.filter_patient_id}} if request.filter_patient_id else None
    )

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=request.top_k,
        where=where_filter,
        include=["documents", "metadatas", "distances"],
    )

    docs = [
        {
            "id": results["ids"][0][i],
            "document": results["documents"][0][i],
            "metadata": results["metadatas"][0][i],
            "distance": results["distances"][0][i],
        }
        for i in range(len(results["ids"][0]))
    ]

    answer = _build_answer(request.query, docs)

    retrieved = [
        RetrievedDocument(
            id=d["id"],
            text=d["document"],
            score=round(1 - d["distance"], 4),
            patient_id=d["metadata"].get("patient_id"),
            chief_complaint=d["metadata"].get("chief_complaint"),
            no_show_risk=float(d["metadata"].get("no_show_risk", 0)),
        )
        for d in docs
    ]

    return QueryResponse(
        query=request.query,
        answer=answer,
        retrieved_docs=retrieved,
        model_used=EMBEDDING_MODEL,
        latency_ms=round((time.perf_counter() - t0) * 1000, 2),
    )


@app.get("/patient/{patient_id}/risk", response_model=PatientRiskResponse)
def patient_risk(patient_id: str) -> PatientRiskResponse:
    collection = get_chroma()
    model = get_embed_model()

    # Retrieve this patient's clinical notes
    results = collection.query(
        query_embeddings=[model.encode(f"patient risk factors no-show history").tolist()],
        n_results=3,
        where={"patient_id": {"$eq": patient_id}},
        include=["documents", "metadatas", "distances"],
    )

    if not results["ids"][0]:
        raise HTTPException(status_code=404, detail=f"No clinical notes for patient {patient_id}")

    notes_context = " ".join(results["documents"][0])

    # Build a risk summary from context
    risk_scores = [float(m.get("no_show_risk", 0.5)) for m in results["metadatas"][0]]
    avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0.5

    risk_tier = "LOW" if avg_risk < 0.3 else "MEDIUM" if avg_risk < 0.6 else "HIGH"

    top_factors = []
    if "non-adherent" in notes_context.lower():
        top_factors.append("History of non-adherence to treatment plan")
    if "missed" in notes_context.lower():
        top_factors.append("Prior missed appointments documented")
    if "transportation" in notes_context.lower():
        top_factors.append("Transportation barriers noted")
    if "anxiety" in notes_context.lower() or "depression" in notes_context.lower():
        top_factors.append("Mental health comorbidities present")
    if not top_factors:
        top_factors = ["No significant risk factors identified in recent notes"]

    recommendation = {
        "LOW": "Standard reminder protocol. No special intervention needed.",
        "MEDIUM": "Send SMS + email reminder 48h and 24h before appointment.",
        "HIGH": "Phone outreach recommended. Consider transportation assistance.",
    }[risk_tier]

    summary = _build_answer(
        f"Summarize this patient's no-show risk factors in 2 sentences.",
        [{"document": notes_context}],
    )

    return PatientRiskResponse(
        patient_id=patient_id,
        no_show_probability=round(avg_risk, 3),
        risk_tier=risk_tier,
        top_risk_factors=top_factors,
        recommendation=recommendation,
        supporting_notes_summary=summary,
        model_version="1.0.0",
    )


@app.post("/embed", response_model=EmbedResponse)
def embed_note(request: EmbedRequest) -> EmbedResponse:
    model = get_embed_model()
    collection = get_chroma()

    doc_id = str(uuid.uuid4())
    embedding = model.encode(request.note_text).tolist()

    collection.upsert(
        ids=[doc_id],
        embeddings=[embedding],
        documents=[request.note_text],
        metadatas=[
            {
                "patient_id": request.patient_id,
                "appointment_id": request.appointment_id,
            }
        ],
    )

    log.info("note_embedded", doc_id=doc_id, patient_id=request.patient_id)
    return EmbedResponse(document_id=doc_id, patient_id=request.patient_id, status="ok")
