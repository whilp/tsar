from cli.app import Application, CommandLineMixin
from cli.log import LoggingApp, LoggingMixin
from cli.daemon import DaemonizingMixin

from . import model
from .client import Tsar
from .util import parsedsn

class Command(LoggingApp):

    @property
    def name(self):
        return self.__class__.__name__.lower().replace("_", "-")

    def pre_run(self):
        # SubCommands need to set up their own logging.
        Application.pre_run(self)
        CommandLineMixin.pre_run(self)

class SubCommand(Command):
    
    def __init__(self, main=None, parent=None, **kwargs):
        self.parent = parent
        Command.__init__(self, main, **kwargs)

    def pre_run(self):
        # Let Command run the CommandLineMixin.pre_run()
        Application.pre_run(self)
        LoggingMixin.pre_run(self)

class DaemonizingCommand(DaemonizingMixin, Command):

    def __init__(self, main=None, **kwargs):
        Command.__init__(self, main, **kwargs)
        DaemonizingMixin.__init__(self, **kwargs)
    
    def setup(self):
        Command.setup(self)
        DaemonizingMixin.setup(self)

    def pre_run(self):
        Command.pre_run(self)
        DaemonizingMixin.pre_run(self)

class DaemonizingSubCommand(DaemonizingMixin, SubCommand):
    
    def __init__(self, main=None, **kwargs):
        SubCommand.__init__(self, main, **kwargs)
        DaemonizingMixin.__init__(self, **kwargs)
    
    def setup(self):
        SubCommand.setup(self)
        DaemonizingMixin.setup(self)

    def pre_run(self):
        SubCommand.pre_run(self)

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
