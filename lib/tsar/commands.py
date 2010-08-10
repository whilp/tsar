import cli

class Command(cli.LoggingApp):
    pass

class SubCommand(Command):
    
    def pre_run(self):
        pass

class DaemonizingSubCommand(cli.DaemonizingApp, SubCommand):
    pass
