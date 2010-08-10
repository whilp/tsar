import cli

class Command(cli.LoggingApp):
    pass

class SubCommand(Command):

    def __init__(self, main=None, parent=None, **kwargs):
        self.parent = parent
        super(SubCommand, self).__init__(main, **kwargs)
    
    def pre_run(self):
        pass
