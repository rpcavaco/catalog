import logging


from typing import Optional
from fastapi import FastAPI, Request, HTTPException # Response, 
from dbase import DBPool, sql_build_sel
from copy import copy

from config import SQL_SEL_AGSNODES, SQL_PESQUISA_GERAL
# from generate import gentable

DECODE_FORMAT = "latin-1"

def create_app():

	logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', filename='catalog.log',level=logging.DEBUG)
	logger = logging.getLogger()

	fapp = FastAPI()
	dbp = DBPool("conncfg.json")

	@fapp.middleware("http")
	async def db_session_middleware(request: Request, call_next):
		request.state.dbpool = dbp.pool
		response = await call_next(request)
		return response
		
	@fapp.on_event("startup")
	async def startup():
		await dbp.openup()

	@fapp.on_event("shutdown")
	async def shutdown():
		await dbp.teardown()

	@fapp.get("/")
	async def hello(request: Request):
		print(request.state.dbpool)  


	@fapp.get("/nodes")
	async def qry(request: Request):

		sqlstr = SQL_SEL_AGSNODES

		readres = []
		async with request.state.dbpool.acquire() as conn:
			async with conn.transaction():
				readres = await conn.fetch(sqlstr)

		return readres

	@fapp.get("/qry")
	async def qry(request: Request, q: str):

		sqlstr_nodes = SQL_SEL_AGSNODES
		sqlstr = SQL_PESQUISA_GERAL
		params = [q, q]

		nodesres = []
		nodes = {}
		readres = []
		outres = []
		async with request.state.dbpool.acquire() as conn:
			async with conn.transaction():
				nodesres = await conn.fetch(sqlstr_nodes)
				readres = await conn.fetch(sqlstr, *params)

		for node in nodesres:
			nodes[node["node"]] = node["internalurl"]

		for rec in readres:
			outres.append(dict(rec))
			outres[-1]["mapservice_url"] = "/".join((nodes[rec["node"]], rec["arcgisserver"]))

		return outres

	# Middleware a aplicar a todos os requests
	@fapp.middleware("http")
	async def case_sens_middleware(request: Request, call_next):
		logger = logging.getLogger()
		raw_query_str = request.scope["query_string"].decode(DECODE_FORMAT)

		if len(raw_query_str) > 0:
			new_qs_buffer = []
			splits1 = raw_query_str.split("&")
			for sp1 in splits1:
				splits2 = sp1.split("=")
				try:
					new_qs_buffer.append("{0}={1}".format(splits2[0].lower(), splits2[1]))
				except:
					logger.debug("raw_query_str:{} splits1:{} splits2:{}".format(raw_query_str, splits1, splits2))
					raise	

			new_query_str = "&".join(new_qs_buffer)	
			request.scope["query_string"] = new_query_str.encode(DECODE_FORMAT)

		# path = request.scope["path"].lower()
		# request.scope["path"] = path

		response = await call_next(request)
		return response 

	return fapp	

app = create_app()

# if __name__ == "__main__":
#	teste_xml()


