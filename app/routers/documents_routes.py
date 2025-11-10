# app/routers/documents_routes.py
import os
from typing import List
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status, Request
from sqlalchemy.orm import Session
from app.database.deps import get_db
from app.models.cases import Case
from app.models.documents import Document
from app.schemas.document_schema import DocumentCreate, DocumentOut
from app.utils.auth import get_current_user_id

router = APIRouter(prefix="/documents", tags=["documents"])

@router.post("/register", response_model=DocumentOut, status_code=status.HTTP_201_CREATED)
def register_document(
    doc: DocumentCreate,
    case_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    # MVP: si no hay sesión, usa 3 (tu usuario) para pruebas
    user_id = get_current_user_id(request) or 3

    case = db.get(Case, case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Case no encontrado")

    if not doc.filename or not doc.original_path or not doc.stored_path:
        raise HTTPException(status_code=400, detail="filename, original_path y stored_path son requeridos")

    os.makedirs(os.path.dirname(doc.original_path), exist_ok=True)
    os.makedirs(os.path.dirname(doc.stored_path), exist_ok=True)

    d = Document(
        case_id=case.id,
        user_id=user_id,
        filename=doc.filename,
        original_path=doc.original_path,
        stored_path=doc.stored_path,
        mime_type=doc.mime_type,
        size_bytes=doc.size_bytes,
        pages=doc.pages,
        status=doc.status,
        notes=doc.notes,
    )
    db.add(d)
    case.doc_count = (case.doc_count or 0) + 1
    db.add(case)
    db.commit()
    db.refresh(d)
    return d

@router.post("/upload", response_model=DocumentOut, status_code=status.HTTP_201_CREATED)
async def upload_document(
    request: Request,
    case_id: int = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    # MVP: si no hay sesión, usa 3 (tu usuario) para pruebas
    user_id = get_current_user_id(request) or 3

    case = db.get(Case, case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Case no encontrado")

    original_dir = os.path.join(case.input_dir, "original")
    stored_dir   = os.path.join(case.index_dir, "clean")
    os.makedirs(original_dir, exist_ok=True)
    os.makedirs(stored_dir,   exist_ok=True)

    original_path = os.path.join(original_dir, file.filename)
    stored_path   = os.path.join(stored_dir, file.filename)

    with open(original_path, "wb") as f:
        f.write(await file.read())

    d = Document(
        case_id=case.id,
        user_id=user_id,
        filename=file.filename,
        original_path=original_path,
        stored_path=stored_path,
        mime_type=file.content_type,
        size_bytes=None,
        pages=None,
        status="queued",
        notes=None,
    )
    db.add(d)
    case.doc_count = (case.doc_count or 0) + 1
    db.add(case)
    db.commit()
    db.refresh(d)
    return d

@router.get("/by_case/{case_id}", response_model=List[DocumentOut])
def list_documents_by_case(case_id: int, db: Session = Depends(get_db)):
    """Lista documentos de un caso."""
    return (
        db.query(Document)
        .filter(Document.case_id == case_id)
        .order_by(Document.id.asc())
        .all()
    )

@router.get("/{document_id}", response_model=DocumentOut)
def get_document(document_id: int, db: Session = Depends(get_db)):
    """Obtiene un documento por id."""
    doc = db.get(Document, document_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document no encontrado")
    return doc
