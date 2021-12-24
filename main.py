from httpx import AsyncClient
from anyio import run


async def login(base_url, email, password):
    async with AsyncClient(base_url=base_url) as ac:
        response = await ac.post(
            "/user/login",
            json={"email": email, "password": password},
        )
        if not response.status_code == 200:
            raise ValueError("Invalid credentials")

        access_token = response.json()["access_token"]
        return access_token


async def list_assets(base_url, access_token):
    async with AsyncClient(base_url=base_url) as ac:
        headers = {"Authorization": "Bearer " + access_token}
        response = await ac.post("/assets", headers=headers)
        if not response.status_code == 200:
            raise ValueError(f'Http error {response.status_code} ' + response.text)
        assets = response.json()
        return assets


async def download_sample(base_url: str, access_token: str, asset_id: int, file: str):
    async with AsyncClient(base_url=base_url) as ac:
        headers = {"Authorization": "Bearer " + access_token}
        response = await ac.post(
            f"/download-sample/{asset_id}", headers=headers
        )
        if not response.status_code == 200:
            raise ValueError(f'Http error {response.status_code} ' + response.text)
        with open(file, "wb") as f:
            f.write(response.content)


async def create_order(base_url: str, access_token: str, asset_id: int):
    async with AsyncClient(base_url=base_url) as ac:
        headers = {"Authorization": "Bearer " + access_token}
        response = await ac.post("/order-asset/1", headers=headers)
        if not response.status_code == 200:
            raise ValueError(f'Http error {response.status_code} ' + response.text)

        order_id = response.json()["order_id"]
        return order_id


async def download_asset(base_url: str, access_token: str, asset_id: int, order_id: str, file: str):
    async with AsyncClient(base_url=base_url) as ac:
        headers = {"Authorization": "Bearer " + access_token}
        response = await ac.post(
            "/download-asset/1",
            json={"order_id": order_id},
            headers=headers,
        )
        if not response.status_code == 200:
            raise ValueError(f'Http error {response.status_code} ' + response.text)
        with open(file, "wb") as f:
            f.write(response.content)


async def main():
    base_url = 'http://marketplace.pangeamt.com:8080'
    user_email = 'test@test.com'
    user_password = 'test'

    # Login
    access_token = await login(base_url, user_email, user_password)

    # List assets
    assets = await list_assets(base_url, access_token)
    print(assets)
    # download sample
    await download_sample(base_url, access_token, 1, 'sample-1.tsv')

    # Create an order for that asset (necessary to download)
    order_id = await create_order(base_url, access_token, 1)

    # Download full asset
    await download_asset(base_url, access_token, 1, order_id, 'asset-1.tsv')


run(main)
