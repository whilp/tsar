from .commands import Command, SubCommand

class Tsar(Command):
    name = "tsar"

    def __init__(self, main=None, commands={}, **kwargs):
        super(Tsar, self).__init__(main, **kwargs)
        self.commands = commands
    
    @staticmethod
    def main(self):
        cmd = self.commands[self.params.command]
        cmd.params = self.params
        return cmd.run()

    def setup(self):
        super(Tsar, self).setup()
        self.subparsers = self.argparser.add_subparsers(dest="command")
        for k, v in self.commands.items():
            command = v(parent=self)
            command.setup()
            self.commands[k] = command

def run():
    from .manage import Clean, Last
    tsar = Tsar(commands={
        "clean": Clean,
        "last": Last,
    })
    tsar.setup()
    tsar.run()

if __name__ == "__main__":
    run()
