import cli

from . import model
from .client import Tsar
from .util import parsedsn

class Command(cli.LoggingApp):

    @property
    def name(self):
        return self.__class__.__name__.lower()

class SubCommand(Command):

    def __init__(self, main=None, parent=None, **kwargs):
        self.parent = parent
        super(SubCommand, self).__init__(main, **kwargs)
    
    def pre_run(self):
        pass

class DBMixin(object):

    def setup(self):
        default_dsn = "redis://localhost:6379/0"
        self.add_param("-D", "--dsn", default=default_dsn,
            help="Database connection: "
                "'<driver>://<username>:<password>@<host>:<port>/<database>' " 
                "(default: %s)" % default_dsn)
            
    def pre_run(self):
        dsn = parsedsn(self.params.dsn)
        del(dsn["username"])
        del(dsn["driver"])
        dsn["db"] = dsn.pop("database")
        model.db = model.connect(**dsn)
        self.db = model.db

class ClientMixin(object):
    
    def setup(self):
        self.add_param("-S", "--service", default=self.service,
            help="service URL (default: %s)" % self.service)

    def pre_run(self):
        self.client = Tsar(self.params.service)
