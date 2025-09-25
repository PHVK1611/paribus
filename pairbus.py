import csv
import json
import time
import uuid
import requests
import tempfile
import os
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(
    title="Hospital Bulk Processing System",
   
)

HOSPITAL_API_URL = "https://hospital-directory.onrender.com"
MAX_CSV_SIZE = 20
ALLOWED_EXTENSIONS = {'csv'}

batch_data: Dict = {}

class HospitalResult(BaseModel):
    row: int
    hospital_id: Optional[int] = None
    name: str
    status: str
    error_message: Optional[str] = None

class BulkProcessingResponse(BaseModel):
    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    hospitals: List[HospitalResult]

class BatchStatusResponse(BaseModel):
    batch_id: str
    status: str
    progress: float
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    start_time: str
    end_time: Optional[str] = None
    processing_time_seconds: Optional[float] = None
    batch_activated: bool

class ValidationResponse(BaseModel):
    valid: bool
    total_hospitals: Optional[int] = None
    message: Optional[str] = None
    exceeds_limit: Optional[bool] = None
    limit: Optional[int] = None
    error: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    hospital_api_connection: bool
    timestamp: str
    active_batches: int

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_batch_id() -> str:
    return str(uuid.uuid4())

def read_csv_file(file_path: str) -> List[Dict]:
    hospitals = []
    
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        if not content.strip():
            raise ValueError("CSV file is empty")
        
        file.seek(0)
        
        csv_reader = csv.DictReader(file)
        
        if not csv_reader.fieldnames:
            raise ValueError("CSV file has no headers")
            
        required_headers = {'name', 'address'}
        headers = set(header.strip().lower() for header in csv_reader.fieldnames)
        
        if not required_headers.issubset(headers):
            missing = required_headers - headers
            raise ValueError(f"Missing required headers: {', '.join(missing)}")
        
        row_count = 0
        for row in csv_reader:
            row_count += 1
            
            name = row.get('name', '').strip()
            address = row.get('address', '').strip()
            phone = row.get('phone', '').strip()
            
            if not name:
                raise ValueError(f"Row {row_count}: Name is required")
            if not address:
                raise ValueError(f"Row {row_count}: Address is required")
            
            hospital = {
                'name': name,
                'address': address,
                'phone': phone if phone else None,
                'row_number': row_count
            }
            hospitals.append(hospital)
    
    return hospitals

def create_hospital(hospital_data: Dict, batch_id: str) -> Dict:
    payload = {
        'name': hospital_data['name'],
        'address': hospital_data['address'],
        'phone': hospital_data['phone'],
        'creation_batch_id': batch_id
    }
    
    try:
        response = requests.post(
            f"{HOSPITAL_API_URL}/hospitals/",
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to create hospital: {str(e)}")

def activate_batch(batch_id: str) -> bool:
    try:
        response = requests.patch(
            f"{HOSPITAL_API_URL}/hospitals/batch/{batch_id}/activate",
            timeout=30
        )
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException:
        return False

def process_hospitals(hospitals: List[Dict], batch_id: str) -> Dict:
    start_time = time.time()
    results = []
    processed_count = 0
    failed_count = 0
    
    batch_data[batch_id] = {
        'status': 'processing',
        'total_hospitals': len(hospitals),
        'processed_hospitals': 0,
        'failed_hospitals': 0,
        'start_time': datetime.now().isoformat(),
        'results': []
    }
    
    for hospital in hospitals:
        try:
            hospital_response = create_hospital(hospital, batch_id)
            
            result = {
                'row': hospital['row_number'],
                'hospital_id': hospital_response.get('id'),
                'name': hospital['name'],
                'status': 'created'
            }
            processed_count += 1
            
        except Exception as e:
            result = {
                'row': hospital['row_number'],
                'name': hospital['name'],
                'status': 'failed',
                'error_message': str(e)
            }
            failed_count += 1
        
        results.append(result)
        
        batch_data[batch_id].update({
            'processed_hospitals': processed_count,
            'failed_hospitals': failed_count,
            'results': results
        })
    
    batch_activated = False
    if failed_count == 0:
        batch_activated = activate_batch(batch_id)
        if batch_activated:
            for result in results:
                if result['status'] == 'created':
                    result['status'] = 'created_and_activated'
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    batch_data[batch_id].update({
        'status': 'completed' if failed_count == 0 else 'partial_failure',
        'end_time': datetime.now().isoformat(),
        'processing_time_seconds': round(processing_time, 2),
        'batch_activated': batch_activated
    })
    
    return {
        'batch_id': batch_id,
        'total_hospitals': len(hospitals),
        'processed_hospitals': processed_count,
        'failed_hospitals': failed_count,
        'processing_time_seconds': round(processing_time, 2),
        'batch_activated': batch_activated,
        'hospitals': results
    }

@app.get("/")
async def home():
    return {
        'message': 'Hospital Bulk Processing System',
        'version': '1.0.0',
        'endpoints': {
            'bulk_create': 'POST /hospitals/bulk',
            'batch_status': 'GET /hospitals/batch/{batch_id}/status',
            'validate_csv': 'POST /hospitals/validate-csv',
            'health': 'GET /health'
        },
        'csv_format': 'name,address,phone (phone is optional)',
        'max_hospitals': MAX_CSV_SIZE,
        'docs': '/docs'
    }

@app.post("/hospitals/bulk", response_model=BulkProcessingResponse)
async def bulk_create_hospitals(file: UploadFile = File(...)):
  
    
    if not file.filename or not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    if file.size == 0:
        raise HTTPException(status_code=400, detail="File is empty")
    
    try:
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"upload_{uuid.uuid4().hex}.csv")
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        try:
            hospitals = read_csv_file(file_path)
            
            if len(hospitals) > MAX_CSV_SIZE:
                raise HTTPException(
                    status_code=400,
                    detail=f'Too many hospitals. Found {len(hospitals)}, maximum allowed: {MAX_CSV_SIZE}'
                )
            
            if len(hospitals) == 0:
                raise HTTPException(status_code=400, detail='No valid hospital data found in CSV')
            
            batch_id = generate_batch_id()
            
            result = process_hospitals(hospitals, batch_id)
            
            return BulkProcessingResponse(**result)
            
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Internal error: {str(e)}')

@app.get("/hospitals/batch/{batch_id}/status", response_model=BatchStatusResponse)
async def get_batch_status(batch_id: str):
    
    if batch_id not in batch_data:
        raise HTTPException(status_code=404, detail='Batch ID not found')
    
    data = batch_data[batch_id]
    
    if data['total_hospitals'] > 0:
        progress = (data['processed_hospitals'] + data['failed_hospitals']) / data['total_hospitals']
    else:
        progress = 0.0
    
    return BatchStatusResponse(
        batch_id=batch_id,
        status=data['status'],
        progress=round(progress, 2),
        total_hospitals=data['total_hospitals'],
        processed_hospitals=data['processed_hospitals'],
        failed_hospitals=data['failed_hospitals'],
        start_time=data['start_time'],
        end_time=data.get('end_time'),
        processing_time_seconds=data.get('processing_time_seconds'),
        batch_activated=data.get('batch_activated', False)
    )

@app.post("/hospitals/validate-csv", response_model=ValidationResponse)
async def validate_csv(file: UploadFile = File(...)):
   
    
    if not file.filename or not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
        return ValidationResponse(
            valid=False,
            error="File is empty"
        )
    
    try:
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"validate_{uuid.uuid4().hex}.csv")
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        try:
            hospitals = read_csv_file(file_path)
            
            return ValidationResponse(
                valid=True,
                total_hospitals=len(hospitals),
                message=f'CSV is valid and contains {len(hospitals)} hospital records',
                exceeds_limit=len(hospitals) > MAX_CSV_SIZE,
                limit=MAX_CSV_SIZE
            )
            
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except ValueError as e:
        return ValidationResponse(
            valid=False,
            error=str(e)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Internal error: {str(e)}')

@app.get("/health", response_model=HealthResponse)
async def health_check():
    
    try:
        response = requests.get(f"{HOSPITAL_API_URL}/hospitals/", timeout=10)
        api_healthy = response.status_code == 200
    except Exception:
        api_healthy = False
    
    active_batches = len([b for b in batch_data.values() if b['status'] == 'processing'])
    
    return HealthResponse(
        status='healthy' if api_healthy else 'degraded',
        hospital_api_connection=api_healthy,
        timestamp=datetime.now().isoformat(),
        active_batches=active_batches
    )

@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={'error': 'Endpoint not found'}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={'error': 'Internal server error'}
    )

if __name__ == '__main__':
    import uvicorn
    port = int(os.environ.get('PORT', 1234))
    uvicorn.run(app, host='0.0.0.0', port=port)