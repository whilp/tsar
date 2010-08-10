from .commands import Command

class Tsar(Command):
    name = "tsar"
    
    @staticmethod
    def main(self):
        pass

tsar = Tsar()
tsar.setup()

if __name__ == "__main__":
    tsar.run()
