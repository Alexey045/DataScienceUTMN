import asyncio
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import datetime
from uuid import uuid4
from click import click_hs


async def handle(request: web.Request) -> web.Response:
    date = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S")
    url = request.query.get("url", "")
    ocl = request.query.get("ocl", "")
    oid = request.query.get("oid", "0")
    uid_cookie = request.cookies.get('user_uuid')
    response = web.Response(status=200, text='ok')
    if uid_cookie is None:
        uid_cookie = str(uuid4())
        response.cookies['user_uuid'] = uid_cookie
    click_hs.add_csv(date, url, ocl, oid, uid_cookie)
    return response


# "disable_ssl_verification": true - do nothing!

app = web.Application()
app.add_routes([web.get('/', handle)])

event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(event_loop)

scheduler = AsyncIOScheduler()
scheduler.add_job(click_hs.eject_to_db, 'interval', minutes=1)

if __name__ == '__main__':
    click_hs.create_buffer()
    scheduler.start()
    web.run_app(app, host="127.0.0.1", port=8000, loop=event_loop)
