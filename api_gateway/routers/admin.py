from fastapi import APIRouter, Depends, HTTPException

from api_gateway.auth import verify_admin, create_api_key, delete_api_key
from api_gateway.schemas import AdminKeyCreate, AdminKeyResponse

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post(
    "/keys",
    response_model=AdminKeyResponse,
    status_code=201,
    dependencies=[Depends(verify_admin)],
)
async def issue_api_key(body: AdminKeyCreate):
    plaintext, key_id = await create_api_key(body.owner_email)
    return AdminKeyResponse(
        api_key=plaintext, key_id=key_id, owner_email=body.owner_email
    )


@router.delete("/keys/{key_id}", status_code=204, dependencies=[Depends(verify_admin)])
async def revoke_api_key(key_id: str):
    if not await delete_api_key(key_id):
        raise HTTPException(status_code=404, detail="Key not found")
