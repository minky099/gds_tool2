setting = {
    'filepath': __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': 'main',
    'menu': {
        'uri': __package__,
        'name': 'GDS 배치 복사',
        'list': [
            {
                'uri': 'main',
                'name': '배치 복사',
                'list': [
                    {'uri': 'setting', 'name': '설정/실행'},
                ]
            },
            {
                'uri': 'log',
                'name': '로그',
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
    from .mod_main import ModuleMain
    P.set_module_list([ModuleMain])
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())

logger = P.logger
