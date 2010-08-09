import cli
    
@cli.LoggingApp
def manage(app):
    cmd = app.commands[app.params.command]
    return cmd()

manage.commands = {
}

manage.add_param("command", choices=manage.commands, help="sub command")

if __name__ == "__main__":
    manage.run()
