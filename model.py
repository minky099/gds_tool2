from datetime import datetime

from .setup import *


class ModelBatchHistory(ModelBase):
    P = P
    __tablename__ = f'{P.package_name}_batch_history'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = P.package_name

    id              = db.Column(db.Integer, primary_key=True)
    created_time    = db.Column(db.DateTime)
    finished_time   = db.Column(db.DateTime)
    folder_id       = db.Column(db.String)
    max_batch_gb    = db.Column(db.String)
    total_files     = db.Column(db.Integer)
    total_batches   = db.Column(db.Integer)
    success_count   = db.Column(db.Integer)
    fail_count      = db.Column(db.Integer)
    status          = db.Column(db.String)   # running | completed | stopped | error
    note            = db.Column(db.String)

    def __init__(self, folder_id='', max_batch_gb=''):
        self.created_time   = datetime.now()
        self.folder_id      = folder_id
        self.max_batch_gb   = max_batch_gb
        self.total_files    = 0
        self.total_batches  = 0
        self.success_count  = 0
        self.fail_count     = 0
        self.status         = 'running'
        self.note           = ''


class ModelSourceBookmark(ModelBase):
    P = P
    __tablename__ = f'{P.package_name}_source_bookmark'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = P.package_name

    id           = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    name         = db.Column(db.String)
    source_id    = db.Column(db.String)

    def __init__(self, name='', source_id=''):
        self.created_time = datetime.now()
        self.name         = name
        self.source_id    = source_id
