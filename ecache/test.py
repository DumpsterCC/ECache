import redis
import traceback
import toml
from sqlalchemy import Column, Index, text
from sqlalchemy.dialects.mysql import TINYINT, INTEGER, VARCHAR
from sqlalchemy.sql.schema import DefaultClause
from sqlalchemy.sql.elements import TextClause
from ecache.core import cache_mixin
from ecache.db import db_manager, model_base

config = toml.load('local.toml').get('mysql', {})
mysql_dsn = "mysql://{user}:{password}@{host}:{port}/{db}?{params}".format(
    host=config.get("host", '127.0.0.1'),
    port=config.get("port", 3306),
    user=config.get("user", 'root'),
    password=config.get("password", 'root'),
    db=config.get("database", 'db'),
    params=config.get("params", "")
)

DB_SETTINGS = {
    'test': {
        'urls': {
            'master': mysql_dsn,
            'slave': mysql_dsn
        },
        'max_overflow': -1,
        'pool_size': 10,
        'pool_recycle': 1200
    }
}
# alsosee :class:`ecache.db.DBManager`
db_manager.create_sessions(DB_SETTINGS)
DBSession = db_manager.get_session('test')
cache_client = redis.StrictRedis()
CacheMixin = cache_mixin(cache_client, DBSession)
DeclarativeBase = model_base()


class UserModel(DeclarativeBase, CacheMixin):
    __tablename__ = 'user'
    TABLE_CACHE_EXPIRATION_TIME = 20
    id = Column('id', INTEGER(display_width=11), primary_key=True, nullable=False)
    status = Column('status', TINYINT(display_width=1), nullable=False, server_default=DefaultClause(TextClause('0')))
    code = Column('code', VARCHAR(length=8), nullable=False)
    mobile = Column('mobile', VARCHAR(length=11), nullable=False, server_default=DefaultClause(TextClause('0')))
    password = Column('password', VARCHAR(length=64), nullable=False, server_default=DefaultClause(TextClause('0')))
    create_ts = Column('create_ts', INTEGER(display_width=11, unsigned=True), nullable=False,
                       server_default=DefaultClause(TextClause('0')))
    __table_args__ = (
        Index('create_ts_status', Column('create_ts', INTEGER(display_width=11, unsigned=True), nullable=False,
                                         server_default=DefaultClause(TextClause('0'))),
              Column('status', TINYINT(display_width=1), nullable=False,
                     server_default=DefaultClause(TextClause('0')))),
        Index('mobile_UNIQUE',
              Column('mobile', VARCHAR(length=11), nullable=False, server_default=DefaultClause(TextClause('0'))),
              Column('code', VARCHAR(length=8), nullable=False), unique=True),
    )

    @classmethod
    def insert(cls, obj):
        s = cls._db_session()
        s.add(obj)
        s.flush()

    @classmethod
    def update(cls, obj):
        s = cls._db_session()
        s.merge(obj)
        s.flush()

    @classmethod
    def select_pk_list(cls, condition_string='', order_by_string=''):
        # TODO
        pass

    def select_list(self, condition_string, order_by_string, readonly=True):
        # TODO
        pass


if __name__ == '__main__':
    print(mysql_dsn)
    print('*' * 50, 'before update user1 user2, query from cache or db')
    try:
        user1 = UserModel.get(100003)
        user2 = UserModel.get(99999999)
        print('user1:', user1.id, user1.status, user1.code, user1.mobile, user1.password)
        print('user2:', user2.id, user2.status, user2.code, user2.mobile, user2.password)
        user1 = UserModel(id=100003, status=0, code='+86', mobile='123', password='1234')
        user2 = UserModel(id=99999999, status=0, code='+86', mobile='321', password='1234')
        UserModel.update(user1)
        UserModel.update(user2)
        DBSession().commit()
    except Exception as e:
        traceback.print_exc()
        DBSession().rollback()

    print('*' * 50, 'after update user1 user2, flush cache and query from db')
    user1 = UserModel.get(100003)
    user2 = UserModel.get(99999999)
    print('user1:', user1.id, user1.status, user1.code, user1.mobile, user1.password)
    print('user2:', user2.id, user2.status, user2.code, user2.mobile, user2.password)
