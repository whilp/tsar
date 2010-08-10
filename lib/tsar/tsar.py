from .commands import Command

class Tsar(Command):
    name = "tsar"

    def __init__(self, *args, **kwargs):
        super(Tsar, self).__init__(*args, **kwargs)
        self.setup()
    
    @staticmethod
    def main(self):
        pass

tsar = Tsar()

if __name__ == "__main__":
    tsar.run()
