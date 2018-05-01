from aiohttp import web
import json

import runner
import analysis

def jsonresponse(func):
    async def inner(*args, **kwargs):
        result = await func(*args, **kwargs)
        return web.Response(text=json.dumps(result))
    return inner

def build_app():
    df = runner.DataFlock()

    def logger(*args, **kwargs):
        print(*(list(args) + [kwargs]))

    def get_env(request):
        try:
            env = df.environment_get(request.match_info['env'])
        except KeyError as e:
            raise web.HTTPBadRequest(text=str(e))
        return env

    @jsonresponse
    async def list_environments(request):
        return df.list_environments()

    @jsonresponse
    async def create_environment(request):
        data = await request.json()

        if not 'name' in data:
            raise web.HTTPBadRequest(text="missing name")

        try:
            env = df.environment_create(data['name'])
        except KeyError as e:
            raise web.HTTPBadRequest(text=str(e))

        env.set_callback(logger)

        return data['name']

    @jsonresponse
    async def create_cell(request):
        data = await request.json()

        if not 'code' in data:
            raise web.HTTPBadRequest(text="missing name")
        code = data['code']

        env = get_env(request)
        
        try:
            cell_id = env.cell_create(analysis.Cell(code))
        except NameError as e:
            raise web.HTTPBadRequest(text=str(e))

        return cell_id

    @jsonresponse
    async def update_cell(request):
        data = await request.json()

        if not 'code' in data:
            raise web.HTTPBadRequest(text="missing name")
        code = data['code']

        env = get_env(request)
        
        try:
            env.cell_update(
                request.match_info['cell_id'],
                analysis.Cell(code)
            )
        except NameError as e:
            raise web.HTTPBadRequest(text=str(e))

        return

    @jsonresponse
    async def get_variable(request):
        env = get_env(request)
        
        try:
            value = env.get_variable(request.match_info['name'])
        except NameError as e:
            raise web.HTTPBadRequest(text=str(e))

        print("got", value)
        return value
        
    app = web.Application()
    app.add_routes([web.get('/', list_environments)])
    app.add_routes([web.post('/', create_environment)])
    app.add_routes([web.post('/{env}/cells', create_cell)])
    app.add_routes([web.post('/{env}/cells/{cell_id}', update_cell)])
    app.add_routes([web.get('/{env}/variables/{name}', get_variable)])
    
    
    return app

if __name__ == "__main__":
    app = build_app()
    web.run_app(app)
