import { ProxyStorage } from '../src/up-storage';

require('@verdaccio/logger').setup([]);

function setupProxy(host, uplinkConf, appConfig) {
  uplinkConf.url = host;

  return new ProxyStorage(uplinkConf, appConfig);
}

describe('Use proxy', () => {
  describe('basic tets', () => {
    test('should do not define proxy', () => {
      const x = setupProxy('http://registry.domain.org', {}, {});

      expect(x.proxy).toEqual(undefined);
    });

    test('uplink configuration should take priority', () => {
      expect(
        setupProxy(
          'http://registry.domain.org',
          { http_proxy: 'registry.local.org' },
          { http_proxy: 'registry.domain.org' }
        ).proxy
      ).toEqual('registry.local.org');
    });

    test('global configuration should be used', () => {
      expect(
        setupProxy('http://registry.domain.org', {}, { http_proxy: 'registry.domain.org' }).proxy
      ).toEqual('registry.domain.org');
    });
  });

  describe('no_proxy invalid cases', () => {
    test('no_proxy is null', () => {
      let x = setupProxy('http://x/x', { http_proxy: 'registry.local.org', no_proxy: null }, {});
      expect(x.proxy).toEqual('registry.local.org');
    });

    test('no_proxy is empty array', () => {
      let x = setupProxy('http://x/x', { http_proxy: 'registry.local.org', no_proxy: [] }, {});
      expect(x.proxy).toEqual('registry.local.org');
    });

    test('no_proxy is empty object', () => {
      let x = setupProxy('http://x/x', { http_proxy: 'registry.local.org', no_proxy: '' }, {});
      expect(x.proxy).toEqual('registry.local.org');
    });

    test('no_proxy - simple/include', () => {
      let x = setupProxy(
        'http://localhost',
        { http_proxy: 'registry.local.org' },
        { no_proxy: 'localhost' }
      );

      expect(x.proxy).toEqual(undefined);
    });

    test('no_proxy - simple/not', () => {
      let x = setupProxy(
        'http://localhost',
        { http_proxy: 'registry.local.org' },
        { no_proxy: 'blah' }
      );

      expect(x.proxy).toEqual('registry.local.org');
    });

    test('no_proxy is boolean', () => {
      let x = setupProxy(
        'http://registry.some.domain',
        { http_proxy: 'registry.local.org', no_proxy: false },
        {}
      );
      expect(x.proxy).toEqual('registry.local.org');
    });
  });

  describe('no_proxy override http_proxy use cases', () => {
    test('no_proxy - various, single string', () => {
      let x = setupProxy(
        'http://blahblah',
        { http_proxy: 'registry.local.org' },
        { no_proxy: 'blah' }
      );

      expect(x.proxy).toEqual('registry.local.org');
    });
    test('should disable proxy if match hostname', () => {
      let x = setupProxy(
        'http://registry.local.org',
        {},
        { http_proxy: '123', no_proxy: 'registry.local.org' }
      );
      expect(x.proxy).toEqual(undefined);
    });
    test('should not override http_proxy if domain does not match', () => {
      let x = setupProxy(
        'http://blahblah',
        {},
        { http_proxy: 'http://registry.local.org', no_proxy: '.blah' }
      );
      expect(x.proxy).toEqual('http://registry.local.org');
    });
    test('should override http_proxy if match domain no_proxy', () => {
      let x = setupProxy('http://blah.blah', { http_proxy: '123', no_proxy: '.blah' }, {});
      expect(x.proxy).toEqual(undefined);
    });
    test('should override http_proxy due no_proxy match with hostname', () => {
      let x = setupProxy('http://blah', { http_proxy: '123', no_proxy: '.blah' }, {});
      expect(x.proxy).toEqual(undefined);
    });
    test('should not override http_proxy if no_proxy does not match', () => {
      let x = setupProxy(
        'http://blahh',
        { http_proxy: 'http://registry.local.org', no_proxy: 'blah' },
        {}
      );
      expect(x.proxy).toEqual('http://registry.local.org');
    });
  });
  describe('no_proxy as array of domains', () => {
    test('should not override http_proxy if not match domain', () => {
      let x = setupProxy(
        'http://blahblah',
        { http_proxy: 'registry.local.org' },
        { no_proxy: 'foo,bar,blah' }
      );

      expect(x.proxy).toEqual('registry.local.org');
    });
    test('should disable proxy if match domain', () => {
      let x = setupProxy('http://blah.blah', { http_proxy: '123' }, { no_proxy: 'foo,bar,blah' });
      expect(x.proxy).toEqual(undefined);
    });

    test('disable proxy if match domain .foo', () => {
      let x = setupProxy('http://blah.foo', { http_proxy: '123' }, { no_proxy: 'foo,bar,blah' });
      expect(x.proxy).toEqual(undefined);
    });
    test('should not disable http_proxy if not match domain', () => {
      let x = setupProxy('http://foo.baz', { http_proxy: '123' }, { no_proxy: 'foo,bar,blah' });
      expect(x.proxy).toEqual('123');
    });
    test('no_proxy should not find match no_proxy as array invalid domains', () => {
      let x = setupProxy(
        'http://blahblah',
        { http_proxy: 'registry.local.org' },
        { no_proxy: ['foo', 'bar', 'blah'] }
      );
      expect(x.proxy).toEqual('registry.local.org');
    });
    test('no_proxy should find match no_proxy as array valid domains', () => {
      let x = setupProxy(
        'http://blah.blah',
        { http_proxy: 'registry.local.org' },
        { no_proxy: ['foo', 'bar', 'blah'] }
      );
      expect(x.proxy).toEqual(undefined);
    });
  });

  describe('no_proxy with ports', () => {
    test('no_proxy - hostport', () => {
      let x = setupProxy('http://localhost:80', { http_proxy: '123' }, { no_proxy: 'localhost' });

      expect(x.proxy).toEqual(undefined);
      x = setupProxy('http://localhost:8080', { http_proxy: '123' }, { no_proxy: 'localhost' });
      expect(x.proxy).toEqual(undefined);
    });
  });

  describe('no_proxy with https match', () => {
    test('no_proxy - secure', () => {
      let x = setupProxy('https://something', { http_proxy: '123' }, {});

      expect(x.proxy).toEqual(undefined);
      x = setupProxy('https://something', { https_proxy: '123' }, {});
      expect(x.proxy).toEqual('123');
      x = setupProxy('https://something', { http_proxy: '456', https_proxy: '123' }, {});
      expect(x.proxy).toEqual('123');
    });
  });
});
