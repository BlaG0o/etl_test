import sys
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from json import load, dump
from models.message import Message

engine = create_engine("postgresql://blag0:postgreParola@192.168.0.199/test")

class ETL:
    def source(self, fname=None, min_val=0, max_val=1000000,
                from_database=None, db_engine=None, db_dialect="", db_driver="", db_username="", db_password="", db_host="", db_port="", db_database=""):
        if fname is not None:
            self._source = self.get_msg(fname)
        elif from_database:
            if db_engine:
                self.source_engine = db_engine
            else:
                if db_driver != "":
                    db_driver = f"+{db_driver}"
                if db_port != "":
                    db_port = f":{db_port}"

                self.source_engine = self.create_engine(db_dialect, db_driver, db_username, db_password, db_host, db_port, db_database)

            Base.metadata.create_all(self.source_engine)

            self._source = self.get_db_msg()
        else:
            self.is_test = True
            self._source = self.get_random_msg()

        self.min_val = min_val
        self.max_val = max_val

        return self

    def sink(self, to_console=True, 
            to_database=None, db_engine=None, db_dialect="", db_driver="", db_username="", db_password="", db_host="", db_port="", db_database="", 
            to_json=None, fname=None):
        if to_json:
            if to_console:
                try:
                    with open(fname, "r") as fp:
                        data = load(fp)
                        Message._count = int(data[-1]["key"][1:]) + 1
                except:
                    pass
            self.to_json_file = True
            self.json_fname = fname
            self._sink = self.write_json_file
        elif to_database:
            if db_engine:
                self.sink_engine = db_engine
            else:
                if db_driver != "":
                    db_driver = f"+{db_driver}"
                if db_port != "":
                    db_port = f":{db_port}"

                self.sink_engine = self.create_engine(db_dialect, db_driver, db_username, db_password, db_host, db_port, db_database)

            Base.metadata.create_all(self.sink_engine)
            session = self.get_session(self.sink_engine)
            Message._count = (session.query(func.max(Message._id)).first()[0] or 0) + 1

            self._sink = self.write_db
        else:
            self._sink = self.write_console
        return self

    def write_db(self,msg):
        session = self.get_session(self.sink_engine)
        session.add(Message.from_dict(msg.to_json()))
        session.commit()
        
    def write_console(self,msg):
        sys.stdout.write(str(msg.to_json()))
        sys.stdout.write("\n")

    def write_json_file(self,msg):
        try:
            with open(self.json_fname, "r") as fp:
                data = load(fp)
        except:
            data = []

        data.append(msg.to_json())
        with open(self.json_fname, "w") as fp:
            dump(data, fp, indent=4)

    def get_msg(self,fname):
        with open(fname, "r") as fp:
            data = load(fp)

        for i,msg in enumerate(data,1):
            yield Message.from_dict(msg)

    def get_db_msg(self):
        session = self.get_session(self.source_engine)
        data = session.query(Message).all()
        for msg in data:
            yield msg

    def get_random_msg(self):
        while True:
            yield Message.random(min_val=self.min_val, max_val=self.max_val)

    def run(self):
        while True:
            try:
                msg = next(self._source)
                self._sink(msg)
            except StopIteration:
                break
            except Exception as e:
                print(e)
                break

    @classmethod
    def create_engine(cls,db_dialect, db_driver, db_username, db_password, db_host, db_port, db_database):
        connection_string = f"{db_dialect}{db_driver}://{db_username}:{db_password}@{db_host}{db_port}/{db_database}"

        return create_engine(connection_string)

    @classmethod
    def get_session(cls,engine):
        return sessionmaker(bind=engine)()

if __name__ == '__main__':
    #Simulation >> Console
    ETL().source().sink().run()

    #Simulation >> JSON File
    ETL().source().sink(to_json=True, fname="output1.json").run()

    #Simulation >> PostgreSQL
    ETL().source().sink(to_database=True, db_engine=engine).run()

    #JSON File >> Console
    ETL().source(fname="source.json").sink().run()

    #JSON File >> JSON File
    ETL().source(fname="source.json").sink(to_json=True, fname="output2.json").run()

    #JSON File >> PostgreSQL
    ETL().source(fname="source.json").sink(to_database=True, db_engine=engine).run()

    #PostgreSQL >> Console
    ETL().source(from_database=True, db_engine=engine).sink().run()

    #PostgreSQL >> JSON File
    ETL().source(from_database=True, db_engine=engine).sink(to_json=True, fname="output3.json").run()

    #PostgreSQL >> PostgreSQL
    ETL().source(
        from_database=True,
        db_dialect="postgresql", 
        db_driver="", 
        db_username="blag0", 
        db_password="postgreParola", 
        db_host="192.168.0.199", 
        db_port="", 
        db_database="test"
    ).sink(
        to_database=True,
        db_dialect="postgresql", 
        db_driver="", 
        db_username="blag0", 
        db_password="postgreParola", 
        db_host="192.168.0.199", 
        db_port="", 
        db_database="test2"
    ).run()