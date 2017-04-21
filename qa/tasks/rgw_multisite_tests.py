"""
rgw multisite testing
"""
import logging
import sys
import pytest

from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology import misc

from rgw_multi import multisite, tests

log = logging.getLogger(__name__)

class RGWMultisiteTests(Task):
    """
    Runs the rgw_multi tests against a multisite configuration created by the
    rgw-multisite task. Tests are run with pytest, using any additional 'args'
    provided.

        - rgw-multisite-tests:
            args: [-k, test_object_sync]

    """
    def __init__(self, ctx, config):
        super(RGWMultisiteTests, self).__init__(ctx, config)

    def setup(self):
        super(RGWMultisiteTests, self).setup()

        if not self.ctx.rgw_multisite:
            raise ConfigError('rgw-multisite-tests must run after the rgw-multisite task')
        realm = self.ctx.rgw_multisite.realm
        master_zone = realm.meta_master_zone()

        # create the test user
        log.info('creating test user..')
        user = multisite.User('rgw-multisite-test-user')
        user.create(master_zone, ['--display-name', 'Multisite Test User',
                                  '--gen-access-key', '--gen-secret'])

        # inject the realm and test user
        tests.init_multi(realm, user)
        tests.realm_meta_checkpoint(realm)

        overrides = self.ctx.config.get('overrides', {})
        misc.deep_merge(self.config, overrides.get('rgw-multisite-tests', {}))

    def begin(self):
        argv = ['--capture=no', '--verbose']
        argv += ['--pyargs'] # pass tests as package names

        # modules included in test discovery
        test_modules = [tests]
        argv += [mod.__name__ for mod in test_modules]

        plugin = loaded_module_plugin(test_modules)

        # extra arguments can be passed as a string or list
        extra_args = self.config.get('args', [])
        if not isinstance(extra_args, list):
            extra_args = [extra_args]
        argv += extra_args

        log.info('running: pytest %s', ' '.join(argv))

        result = pytest.main(argv, [plugin])
        if result != 0:
            raise RuntimeError('rgw multisite test failures [pytest return code=%d]' % result)


def loaded_module_plugin(modules):
    """ builds a pytest plugin that prevents it from re-importing our test
    modules. the tests rely on dependency injection (tests.init_multi(),
    for example), so pytest needs to run tests on our imported copy """

    class LoadedModule(pytest.Module):
        def __init__(self, mod, path, parent):
            super(LoadedModule, self).__init__(path, parent)
            self.mod = mod

        def _importtestmodule(self):
            """ bypass import and return self.mod """
            self.config.pluginmanager.consider_module(self.mod)
            return self.mod

    class LoadedModulePlugin:
        def __init__(self, modules):
            self.modules = modules

        def pytest_pycollect_makemodule(self, path, parent):
            """ create custom collector for matching modules """
            for mod in self.modules:
                modpath = mod.__file__
                if modpath.endswith('.pyc'):
                    modpath = modpath[:-1]
                if path.samefile(modpath): # could be symlink
                    return LoadedModule(mod, path, parent)
            # default module collector
            return pytest.Module(path, parent)

    return LoadedModulePlugin(modules)


task = RGWMultisiteTests
