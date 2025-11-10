# app/routers/cases_routes.py
import os
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from app.database.deps import get_db
from app.models.cases import Case
from app.schemas.case_schema import CaseCreate, CaseOut
from app.utils.auth import get_current_user_id

router = APIRouter(prefix="/cases", tags=["cases"])

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

@router.post("", response_model=CaseOut, status_code=status.HTTP_201_CREATED)
def create_case(payload: CaseCreate, request: Request, db: Session = Depends(get_db)):
    # MVP: si no hay sesión, usa 3 (tu usuario) para pruebas
    user_id = get_current_user_id(request) or 3
    if not payload.input_dir or not payload.index_dir:
        raise HTTPException(status_code=400, detail="input_dir y index_dir son requeridos")

    _ensure_dir(payload.input_dir)
    _ensure_dir(payload.index_dir)

    case = Case(
        user_id=user_id,
        customer_id=payload.customer_id,
        name=payload.name,
        status=payload.status,
        input_dir=payload.input_dir,
        index_dir=payload.index_dir,
        rag_version=payload.rag_version,
        notes=payload.notes,
    )
    db.add(case)
    db.commit()
    db.refresh(case)
    return case

@router.get("", response_model=List[CaseOut])
def list_cases(db: Session = Depends(get_db)):
    """Lista todos los casos (orden asc por id)."""
    return db.query(Case).order_by(Case.id.asc()).all()

@router.get("/{case_id}", response_model=CaseOut)
def get_case(case_id: int, db: Session = Depends(get_db)):
    """Obtiene un caso por id."""
    case = db.get(Case, case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Case no encontrado")
    return case
