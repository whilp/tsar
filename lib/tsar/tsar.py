from operator import itemgetter

from .commands import Command, SubCommand

class Tsar(Command):

    def __init__(self, main=None, commands={}, **kwargs):
        super(Tsar, self).__init__(main=main, **kwargs)
        self.commands = commands
    
    @staticmethod
    def main(self):
        cmd = self.commands[self.params.command]
        cmd.params = self.params
        return cmd.run()

    def setup(self):
        super(Tsar, self).setup()
        self.subparsers = self.argparser.add_subparsers(dest="command")
        for k, v in sorted(self.commands.items(), key=itemgetter(0)):
            command = v(parent=self)
            command.setup()
            self.commands[k] = command

def run():
    from . import manage
    from .collectors.commands import Collect
    from .web import Serve

    tsar = Tsar(commands={
        "clean": manage.Clean,
        "collect": Collect,
        "last": manage.Last,
        "record": manage.Record,
        "serve": Serve,
    })
    tsar.setup()
    tsar.run()

if __name__ == "__main__":
    run()
