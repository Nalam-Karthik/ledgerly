from tests.conftest import PASSWORD, signup


async def test_register_login_me(client):
    user = await signup(client, "alice@test.dev")
    r = await client.get("/auth/me", headers=user["headers"])
    assert r.status_code == 200
    assert r.json()["email"] == "alice@test.dev"
    assert r.json()["role"] == "user"


async def test_duplicate_email_conflicts(client):
    await signup(client, "dup@test.dev")
    r = await client.post("/auth/register", json={"email": "DUP@test.dev", "password": PASSWORD})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "conflict"


async def test_bad_credentials(client):
    await signup(client, "bob@test.dev")
    r = await client.post("/auth/login", json={"email": "bob@test.dev", "password": "wrong-password"})
    assert r.status_code == 401
    r = await client.post("/auth/login", json={"email": "nobody@test.dev", "password": PASSWORD})
    assert r.status_code == 401


async def test_missing_and_garbage_tokens_rejected(client):
    assert (await client.get("/auth/me")).status_code == 401
    r = await client.get("/auth/me", headers={"Authorization": "Bearer not-a-jwt"})
    assert r.status_code == 401


async def test_refresh_token_cannot_be_used_as_access_token(client):
    user = await signup(client)
    r = await client.get("/auth/me", headers={"Authorization": f"Bearer {user['refresh_token']}"})
    assert r.status_code == 401


async def test_refresh_rotation_and_reuse_detection(client):
    user = await signup(client)
    first = user["refresh_token"]

    r = await client.post("/auth/refresh", json={"refresh_token": first})
    assert r.status_code == 200
    second = r.json()["refresh_token"]
    assert second != first

    # Replaying the rotated token looks like theft: it fails AND revokes the newer token too.
    r = await client.post("/auth/refresh", json={"refresh_token": first})
    assert r.status_code == 401
    assert "reuse" in r.json()["error"]["detail"]
    r = await client.post("/auth/refresh", json={"refresh_token": second})
    assert r.status_code == 401


async def test_logout_revokes_session(client):
    user = await signup(client)
    assert (await client.post("/auth/logout", json={"refresh_token": user["refresh_token"]})).status_code == 204
    r = await client.post("/auth/refresh", json={"refresh_token": user["refresh_token"]})
    assert r.status_code == 401


async def test_admin_routes_require_admin_role(client):
    user = await signup(client)
    assert (await client.get("/admin/reconciliation", headers=user["headers"])).status_code == 403
    admin = await signup(client, "admin@test.dev")
    assert (await client.get("/admin/reconciliation", headers=admin["headers"])).status_code == 200
