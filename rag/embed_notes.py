"""
Embed synthetic clinical notes into ChromaDB.

Generates realistic but fully synthetic patient clinical notes using Faker,
embeds them with sentence-transformers (free, local, no API key), and upserts
them into ChromaDB collections for RAG retrieval.

Usage:
  python rag/embed_notes.py --patients 200 --notes-per-patient 3
"""

from __future__ import annotations

import argparse
import os
import random
import uuid
from typing import Any

import chromadb
import structlog
from dotenv import load_dotenv
from faker import Faker
from sentence_transformers import SentenceTransformer

load_dotenv()

log = structlog.get_logger()
fake = Faker()

CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
COLLECTION_NAME = os.getenv("CHROMA_COLLECTION_CLINICAL_NOTES", "clinical_notes")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

CHIEF_COMPLAINTS = [
    "annual wellness visit", "follow-up for hypertension management",
    "diabetes mellitus type 2 quarterly check", "chest pain evaluation",
    "routine physical examination", "knee pain", "back pain",
    "upper respiratory infection", "follow-up for anxiety and depression",
    "medication refill and review", "post-operative follow-up",
    "weight management consultation", "sleep disorder evaluation",
]

ASSESSMENT_TEMPLATES = [
    "Patient is a {age}-year-old {gender} presenting with {complaint}. "
    "Vital signs stable. Blood pressure {bp}. Heart rate {hr} bpm. "
    "Patient reports {symptom_duration} of symptoms. "
    "Assessment: {diagnosis}. Plan: {plan}.",

    "{age}-year-old {gender} with history of {comorbidity} here for {complaint}. "
    "Patient has {missed} missed appointments in the past 90 days. "
    "Discussed importance of follow-up care. {plan}.",

    "Follow-up visit for {comorbidity}. Patient {adherence} to prescribed regimen. "
    "Labs reviewed — {lab_result}. "
    "Patient lives {distance} miles from clinic. Transportation discussed. {plan}.",
]

DIAGNOSES = [
    "Hypertension, well-controlled", "Type 2 diabetes mellitus",
    "Major depressive disorder", "Generalized anxiety disorder",
    "Osteoarthritis of the knee", "Chronic low back pain",
    "Hyperlipidemia", "Obesity (BMI > 30)", "Asthma, mild persistent",
    "GERD", "Hypothyroidism",
]

PLANS = [
    "Continue current medications. Follow up in 3 months.",
    "Adjusted medication dosage. Return in 6 weeks for labs.",
    "Referral to specialist placed. Patient instructed to call with worsening symptoms.",
    "Lifestyle counseling provided. Repeat labs in 3 months.",
    "Physical therapy ordered. NSAIDs for pain management.",
]


def generate_clinical_note(patient_id: str, appointment_id: str) -> dict[str, Any]:
    age = random.randint(25, 85)
    gender = random.choice(["male", "female"])
    complaint = random.choice(CHIEF_COMPLAINTS)
    template = random.choice(ASSESSMENT_TEMPLATES)

    note_text = template.format(
        age=age,
        gender=gender,
        complaint=complaint,
        bp=f"{random.randint(100, 160)}/{random.randint(60, 100)}",
        hr=random.randint(55, 105),
        symptom_duration=random.choice(["2 days", "1 week", "3 months", "6 months"]),
        diagnosis=random.choice(DIAGNOSES),
        plan=random.choice(PLANS),
        comorbidity=random.choice(DIAGNOSES),
        missed=random.randint(0, 4),
        adherence=random.choice(["adherent", "partially adherent", "non-adherent"]),
        lab_result=random.choice([
            "HbA1c 7.2% (improved)", "LDL 118 mg/dL", "TSH 2.4 (normal)",
            "creatinine stable at 1.1", "CBC within normal limits"
        ]),
        distance=round(random.uniform(0.5, 35), 1),
    )

    return {
        "id": str(uuid.uuid4()),
        "text": note_text,
        "metadata": {
            "patient_id": patient_id,
            "appointment_id": appointment_id,
            "age": str(age),
            "gender": gender,
            "chief_complaint": complaint,
            "no_show_risk": str(round(random.uniform(0.05, 0.85), 2)),
        },
    }


def embed_and_upsert(notes: list[dict], model: SentenceTransformer, collection: Any) -> None:
    texts = [n["text"] for n in notes]
    ids = [n["id"] for n in notes]
    metadatas = [n["metadata"] for n in notes]

    embeddings = model.encode(texts, show_progress_bar=False, batch_size=32).tolist()
    collection.upsert(ids=ids, embeddings=embeddings, documents=texts, metadatas=metadatas)


def main() -> None:
    parser = argparse.ArgumentParser(description="Embed clinical notes into ChromaDB")
    parser.add_argument("--patients", type=int, default=100)
    parser.add_argument("--notes-per-patient", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()

    log.info("loading_embedding_model", model=EMBEDDING_MODEL)
    model = SentenceTransformer(EMBEDDING_MODEL)

    client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    total = 0
    patient_ids = [str(uuid.uuid4()) for _ in range(args.patients)]
    batch: list[dict] = []

    for patient_id in patient_ids:
        for _ in range(args.notes_per_patient):
            note = generate_clinical_note(patient_id, appointment_id=str(uuid.uuid4()))
            batch.append(note)
            if len(batch) >= args.batch_size:
                embed_and_upsert(batch, model, collection)
                total += len(batch)
                log.info("batch_upserted", total=total)
                batch = []

    if batch:
        embed_and_upsert(batch, model, collection)
        total += len(batch)

    log.info("embedding_complete", total_notes=total, collection=COLLECTION_NAME)


if __name__ == "__main__":
    main()
