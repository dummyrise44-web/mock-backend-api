from fastapi import FastAPI, Depends, UploadFile, File, Query, HTTPException, Request, Body, Form, Path
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import Table, select, text
from database import SessionLocal, engine, metadata
from pydantic import BaseModel
import csv
from zipfile import ZipFile
import zipfile
import io, string
from typing import Any, Optional, List
from fastapi.responses import JSONResponse, StreamingResponse
import base64
import json
import random
from datetime import datetime, timezone
import os

app = FastAPI()

# Allow requests from your frontend
origins = [
    "http://192.168.88.214:5173",  
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # allow these origins
    allow_credentials=True,
    allow_methods=["*"],  # allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # allow all headers
)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Reflect existing tables
entries = Table("entries", metadata, autoload_with=engine)
regions_table = Table("regions", metadata, autoload_with=engine)
provinces_table = Table("provinces_districts", metadata, autoload_with=engine)
cities_table = Table("cities", metadata, autoload_with=engine)
registration_units = Table("registration_units", metadata, autoload_with=engine)


class RegistrationUnitCreate(BaseModel):
    city_id: int
    name: str
    latitude: float
    longitude: float

class DocumentTransactionPayload(BaseModel):
    entry_id: str
    registration_unit_name: str
    document_id: str  # always required
    doc_type: Optional[str] = None  # only required on first creation
    file_path: Optional[str] = None
    file_name: Optional[str] = None
    transaction_type_name: str
    results: Any  # JSON object
    transaction_status: str

# Upload CSV endpoint with batch inserts
@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    db: Session = SessionLocal()
    try:
        contents = await file.read()
        csv_file = io.StringIO(contents.decode("utf-8"))
        reader = csv.DictReader(csv_file, delimiter=',')

        region_cache = {}
        province_cache = {}
        city_batch = []
        batch_size = 50  # adjust batch size as needed

        for i, row in enumerate(reader, start=1):
            region_name = row["Region"].strip()
            province_name = row["Province"].strip()
            city_name = row["City"].strip()

            # 1️⃣ Region
            if region_name in region_cache:
                region_id = region_cache[region_name]
            else:
                region = db.execute(
                    select(regions_table).where(regions_table.c.name == region_name)
                ).mappings().first()
                if not region:
                    result = db.execute(
                        regions_table.insert().values(name=region_name)
                    )
                    db.commit()
                    region_id = result.inserted_primary_key[0]
                else:
                    region_id = region["id"]
                region_cache[region_name] = region_id

            # 2️⃣ Province
            province_key = f"{region_id}-{province_name}"
            if province_key in province_cache:
                province_id = province_cache[province_key]
            else:
                province = db.execute(
                    select(provinces_table).where(
                        (provinces_table.c.name == province_name) &
                        (provinces_table.c.region_id == region_id)
                    )
                ).mappings().first()
                if not province:
                    result = db.execute(
                        provinces_table.insert().values(
                            name=province_name,
                            region_id=region_id
                        )
                    )
                    db.commit()
                    province_id = result.inserted_primary_key[0]
                else:
                    province_id = province["id"]
                province_cache[province_key] = province_id

            # 3️⃣ City batch insert
            if city_name:
                city_batch.append({
                    "name": city_name,
                    "province_district_id": province_id
                })

            # Insert batch every 500 cities
            if len(city_batch) >= batch_size:
                db.execute(cities_table.insert(), city_batch)
                db.commit()
                print(f"Inserted {i} rows so far...")
                city_batch = []

        # Insert any remaining cities
        if city_batch:
            db.execute(cities_table.insert(), city_batch)
            db.commit()

        return {"message": f"CSV imported successfully! Total rows processed: {i}"}

    finally:
        db.close()


@app.get("/regions/")
def get_regions(db: Session = Depends(get_db)):
    regions = db.execute(select(regions_table)).mappings().all()
    return [{"id": r["id"], "name": r["name"]} for r in regions]

# Get provinces/districts by region
@app.get("/provinces/")
def get_provinces(region_id: int = Query(..., description="ID of the region"), db: Session = Depends(get_db)):
    provinces = db.execute(
        select(provinces_table).where(provinces_table.c.region_id == region_id)
    ).mappings().all()
    
    return [{"id": p["id"], "name": p["name"], "region_id": p["region_id"]} for p in provinces]

# Get cities by province/district
@app.get("/cities/")
def get_cities(province_id: int = Query(..., description="ID of the province/district"), db: Session = Depends(get_db)):
    cities = db.execute(
        select(cities_table).where(cities_table.c.province_district_id == province_id)
    ).mappings().all()
    
    return [{"id": c["id"], "name": c["name"], "province_district_id": c["province_district_id"]} for c in cities]

@app.get("/reg-units/")
def get_regions(db: Session = Depends(get_db)):
    regunit = db.execute(select(registration_units)).mappings().all()
    return [{"id": r["id"], "name": r["name"]} for r in regunit]

@app.post("/registration-unit/")
def create_registration_unit(
    unit: RegistrationUnitCreate, db: Session = Depends(get_db)
):
    # Optional: check if registration unit already exists
    existing = db.execute(
        select(registration_units).where(
            (registration_units.c.city_id == unit.city_id) &
            (registration_units.c.name == unit.name)
        )
    ).mappings().first()

    if existing:
        raise HTTPException(status_code=400, detail="Registration unit already exists for this city")

    result = db.execute(
        registration_units.insert().values(
            city_id=unit.city_id,
            name=unit.name,
            latitude=unit.latitude,
            longitude=unit.longitude
        )
    )
    db.commit()

    return {"message": "Registration unit created successfully", "id": result.inserted_primary_key[0]}

# XML FILES NEEDS TO BE CONVERTED TO BASE 64 BEFORE SAVING TO DATABASE
@app.post("/convert-xml-to-base64-file/")
async def convert_xml_file(file: UploadFile = File(...)):
    contents = await file.read()
    xml_base64 = base64.b64encode(contents).decode("utf-8")
    return {"base64": xml_base64}

# INSERT DOCUMENT PER TRANSACTION IN THE DATABASE
@app.post("/document-transaction/")
def insert_document_transaction(
    payload: DocumentTransactionPayload,
    db: Session = Depends(get_db)
):
    try:
        # Wrap raw SQL in text()
        sql = text("""
            CALL insert_document_transaction_proc(
                :p_entry_id,
                :p_registration_unit_name,
                :p_document_id,
                :p_doc_type,
                :p_file_path,
                :p_file_name,
                :p_transaction_type_name,
                :p_results,
                :p_transaction_status,
                :o_entry_pk,
                :o_transaction_id
            )
        """)

        results_json = json.dumps(payload.results)

        result = db.execute(
            sql,
            {
                "p_entry_id": payload.entry_id, # generated from backend (e.g. ID-2025-0001)
                "p_registration_unit_name": payload.registration_unit_name, # should be existing in databsae already
                "p_document_id": payload.document_id, # generated from backend (e.g. DOC-0001-0001)
                "p_doc_type": payload.doc_type, # should extract from quality (only for first insert of document)
                "p_file_path": payload.file_path, # xml
                "p_file_name": payload.file_name, # orig name
                "p_transaction_type_name": payload.transaction_type_name, # quality, authenticity, consistency, face, signature, duplicate
                "p_results": results_json,  # must be JSON string
                "p_transaction_status": payload.transaction_status, # success, error (based on dan's json)
                "o_entry_pk": None,
                "o_transaction_id": None,
            }
        )

        db.commit()

        return {
            "message": "Document transaction inserted successfully",
            "entry_id": payload.entry_id,
            "document_id": payload.document_id
        }

    except Exception as e:
        db.rollback()
        # Return the exception message from the procedure
        return JSONResponse(
            status_code=400,
            content={"message": str(e)}
        )


# GET NUMBER OF FILES EXTRACTED IN ZIP
@app.post("/process/")
async def process_files(
    files: List[UploadFile] = File(...),
    checks: str = Form(None)  # JSON string from frontend
):
    """
    Accepts multiple ZIP files and a JSON string of selected checks.
    Returns the number of files extracted from each ZIP.
    """
    # Parse checks JSON
    try:
        selected_checks = json.loads(checks) if checks else []
    except json.JSONDecodeError:
        return {"error": "Invalid checks format"}

    response_data = []

    for f in files:
        if not f.filename.lower().endswith(".zip"):
            response_data.append({
                "file_name": f.filename,
                "status": "error",
                "message": "Not a ZIP file",
                "checksRun": selected_checks
            })
            continue

        try:
            contents = await f.read()
            zip_file = ZipFile(io.BytesIO(contents))
            file_count = len(zip_file.namelist())

            response_data.append({
                "file_name": f.filename,
                "status": "success",
                "files_extracted": file_count,
                "checksRun": selected_checks
            })
        except Exception as e:
            response_data.append({
                "file_name": f.filename,
                "status": "error",
                "message": str(e),
                "checksRun": selected_checks
            })

    return {"processed": response_data}


def random_filename(ext="png"):
    return ''.join(random.choices(string.ascii_letters, k=8)) + f".{ext}"

def mock_document_result(ok=True):
    if ok:
        return {
            "status": "success",
            "message": "Document scanned successfully. Quality threshold met.",
            "status_code": 200,
            "code": "QUALITY_200",
            "data": {
                "doc_type": "philsys",
                "similarity_percent": 30.65,
                "quality": {
                    "metric": "mean_absdiff",
                    "score": 0.92,
                    "threshold": 0.85,
                    "ok": True
                },
                "timestamp": datetime.utcnow().isoformat(),
                "runtime_ms": 1072.5
            },
            "filename": random_filename("png")
        }
    else:
        return {
            "status": "error",
            "message": "Document quality score did not meet the required threshold.",
            "status_code": 400,
            "code": "QUALITY_001",
            "details": {
                "doc_type": "philsys",
                "similarity_percent": 41.0,
                "quality": {
                    "metric": "mean_absdiff",
                    "score": 0.837,
                    "threshold": 0.85,
                    "ok": False
                },
                "timestamp": datetime.utcnow().isoformat(),
                "runtime_ms": 253.55
            },
            "filename": random_filename("jpeg")
        }

@app.post("/process-zip/")
async def process_zip_single(
    files: List[UploadFile] = File(...),
    checks: str = Form(None)
):
    """
    Accepts ZIP files and returns JSON per file/entry in the format expected:
    One entry per file with its results inside "data.results".
    """
    selected_checks = checks.split(",") if checks else []
    all_entries = []

    for file in files:
        reg_unit_name = file.filename.replace(".zip", "")
        zip_bytes = await file.read()
        zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))

        for info in zip_file.infolist():
            if not info.filename.endswith(".xml"):
                continue

            xml_name = info.filename.replace(".xml", "")
            results = [
                mock_document_result(True),   # success example
                mock_document_result(False)   # error example
            ]

            entry_json = {
                "status": "success",
                "message": "Batch processed with one or more errors.",
                "status_code": 207,
                "data": {
                    "results": results
                },
                "file_name": xml_name,
                "checksRun": selected_checks,
                "registration_unit": reg_unit_name
            }

            all_entries.append(entry_json)

    return JSONResponse(content=all_entries)


@app.get("/entries/")
def get_entries(
    entry_id: Optional[str] = Query(None, description="Filter by entry ID"),
    status: Optional[str] = Query(None, description="Filter by entry status"),
    registration_unit: Optional[str] = Query(None, description="Filter by registration unit"),
    created_from: Optional[str] = Query(None, description="Filter by created_at from date (YYYY-MM-DD)"),
    created_to: Optional[str] = Query(None, description="Filter by created_at to date (YYYY-MM-DD)"),
    transaction_type: Optional[str] = Query(None, description="Filter by transaction type"),
    db: Session = Depends(get_db)
):
    query = text("""
        SELECT * FROM get_entries(
            :p_entry_id,
            :p_entry_status,
            :p_registration_unit,
            :p_created_at_start,
            :p_created_at_end,
            :p_transaction_type
        )
    """)
    
    result = db.execute(
        query,
        {
            "p_entry_id": entry_id,
            "p_entry_status": status,
            "p_registration_unit": registration_unit,
            "p_created_at_start": created_from,
            "p_created_at_end": created_to,
            "p_transaction_type": transaction_type
        }
    ).mappings().all()

    return result


@app.get("/entries/{entry_id}/flagged-documents/")
def get_flagged_documents(entry_id: str, db: Session = Depends(get_db)):
    query = text("SELECT * FROM get_flagged_documents(:entry_id)")
    result = db.execute(query, {"entry_id": entry_id}).mappings().all()
    return result

@app.get("/documents/{document_id}/error-transactions/")
def get_error_transactions(document_id: str, db: Session = Depends(get_db)):
    query = text("SELECT * FROM get_error_transactions(:document_id)")
    result = db.execute(query, {"document_id": document_id}).mappings().all()
    return result


@app.get("/entries/{entry_id}/summary/")
def get_entry_summary(entry_id: str, db: Session = Depends(get_db)):
    query = text("SELECT * FROM get_entry_summary(:entry_id)")
    result = db.execute(query, {"entry_id": entry_id}).mappings().first()
    if not result:
        raise HTTPException(status_code=404, detail="Entry not found")
    return result

@app.get("/entries/{entry_id}/documents/")
def get_documents_by_entry(entry_id: str, db: Session = Depends(get_db)):
    query = text("SELECT * FROM get_documents_by_entry(:entry_id)")
    result = db.execute(query, {"entry_id": entry_id}).mappings().all()
    return result

@app.get("/{entry_id}/documents/")
def get_documents_filtered(
    entry_id: str = Path(..., description="Entry ID to filter documents"),
    doc_type: Optional[str] = Query(None, description="Filter by document type"),
    document_status: Optional[str] = Query(None, description="Filter by document status"),
    transaction_type: Optional[str] = Query(None, description="Filter by transaction type"),
    db: Session = Depends(get_db)
):
    query = text("""
        SELECT * FROM get_documents_filtered(
            :p_entry_id,
            :p_doc_type,
            :p_document_status,
            :p_transaction_type
        )
    """)

    result = db.execute(
        query,
        {
            "p_entry_id": entry_id,
            "p_doc_type": doc_type,
            "p_document_status": document_status,
            "p_transaction_type": transaction_type
        }
    ).mappings().all()

    return result

@app.get("/{entry_id}/document-transactions/")
def get_document_transaction_results(
    entry_id: str = Path(..., description="Entry ID"),
    document_id: str = Query(..., description="Document ID"),
    transaction_type: str = Query(None, description="Optional transaction type filter"),
    db: Session = Depends(get_db)
):
    query = text("""
        SELECT * FROM get_document_transaction_results(
            :p_entry_id,
            :p_document_id,
            :p_transaction_type
        )
    """)

    result = db.execute(
        query,
        {
            "p_entry_id": entry_id,
            "p_document_id": document_id,
            "p_transaction_type": transaction_type
        }
    ).mappings().all()

    return result