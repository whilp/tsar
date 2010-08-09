import cli

from . import model
from .util import parsedsn

def keys():
	pass
    
@cli.LoggingApp
def manage(app):
    dsn = parsedsn(app.params.dsn)
    del(dsn["username"])
    del(dsn["driver"])
    dsn["db"] = dsn.pop("database")
    model.db = model.connect(**dsn)

    cmd = app.commands[app.params.command]
    return cmd()

manage.commands = {
}

default_dsn = "redis://localhost:6379/0"
manage.add_param("command", choices=manage.commands, help="sub command")
server.add_param("-D", "--dsn", default=default_dsn,
    help="<driver>://<username>:<password>@<host>:<port>/<database> (default: %s)" % default_dsn)

if __name__ == "__main__":
    manage.run()
