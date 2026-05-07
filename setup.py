setting = {
    'filepath': __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': 'main',
    'menu': {
        'uri': __package__,
        'name': 'gds_tool2',
        'list': [
            {
                'uri': 'main',
                'name': '배치 복사',
                'list': [
                    {'uri': 'setting', 'name': '설정/실행'},
                    {'uri': 'history', 'name': '작업 이력'},
                ]
            },
        ]
    },
    'setting_menu': None,
    'default_route': 'normal',
}

import traceback

from plugin import *

P = create_plugin_instance(setting)

try:
    from .model import ModelBatchHistory
    from .mod_main import ModuleMain
    P.ModelBatchHistory = ModelBatchHistory
    P.set_module_list([ModuleMain])
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())

logger = P.logger
