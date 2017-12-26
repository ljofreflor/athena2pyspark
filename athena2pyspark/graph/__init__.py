
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed


class data_node(object):
    def __init__(self, prev, file_type, index=None, name=None):
        self.index = index
        self.name = name
        self.prev = prev
        self.file_type = file_type
        self.pool = ThreadPoolExecutor(len(self.prev))

    def run(self):
        # corremos de forma asincrona todas las tablas necesarias para
        # construir la tabla actual, que a su vez corren todas las tablas
        # necesarias hasta llegar a las tablas mas basicas formadas por la
        # misma fuente.

        promise = [self.pool.submit(s.run) for s in self.prev]
        result = [r.result() for r in as_completed(promise)]

        # ya que corrieron todas las querys anteriores entonces podemos
        # construir la tabla


class async_priorizacion(data_node):
    def __init__(self):
        pass
