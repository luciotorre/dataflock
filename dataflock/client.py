import fire
import requests


class Client(object):

    def __init__(self, server="http://localhost:8080/"):
        self._server = server

    def list_environments(self):
        r = requests.get(self._server)
        r.raise_for_status()
        return r.text

    def create_environment(self, name):
        r = requests.post(self._server, json={'name': name})
        r.raise_for_status()
        return r.text

    def create_cell(self, environment, code, live=True):
        r = requests.post(
            self._server + environment + '/cells', 
            json={'code': code, 'live': live})
        r.raise_for_status()
        return r.text

    def update_cell(self, environment, cell_id, code, live=True):
        r = requests.post(
            self._server + environment + "/cells/" + cell_id, 
            json={'code': code, 'live': live})
        r.raise_for_status()
        return r.text

    def get(self, environment, name):
        r = requests.get(self._server + environment + "/variables/" + name)
        r.raise_for_status()
        return r.text





if __name__ == '__main__':
  fire.Fire(Client)