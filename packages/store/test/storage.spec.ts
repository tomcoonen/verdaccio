import nock from 'nock';
import * as httpMocks from 'node-mocks-http';

import { Config } from '@verdaccio/config';
import { API_ERROR, DIST_TAGS, HEADERS, errorUtils } from '@verdaccio/core';
import { setup } from '@verdaccio/logger';
import { configExample, generateRamdonStorage } from '@verdaccio/mock';
import { generatePackageMetadata } from '@verdaccio/test-helper';

import { Storage } from '../src';
import manifestFooRemoteNpmjs from './fixtures/manifests/foo-npmjs.json';

// import manifestFooRemoteVerdaccio from './fixtures/manifests/foo-verdaccio.json';

setup([]);

const domain = 'http://localhost:4873';
const fakeHost = 'localhost:4873';
const fooManifest = generatePackageMetadata('foo', '1.0.0');

describe('storage', () => {
  beforeEach(() => {
    nock.cleanAll();
    nock.abortPendingRequests();
    jest.clearAllMocks();
  });

  describe('add packages', () => {
    test('add package item', async () => {
      nock(domain).get('/foo').reply(404);
      const config = new Config(
        configExample({
          storage: generateRamdonStorage(),
        })
      );
      const storage = new Storage(config);
      await storage.init(config);

      await storage.addPackage('foo', fooManifest, (err) => {
        expect(err).toBeNull();
      });
    });
  });

  describe('syncUplinksMetadataNext()', () => {
    test('should handle double failure on uplinks with timeout', async () => {
      const fooManifest = generatePackageMetadata('foo', '8.0.0');
      nock('https://registry.npmjs.org').get('/foo').delay(2000).reply(201, manifestFooRemoteNpmjs);
      nock('https://registry.verdaccio.org').get('/foo').replyWithError('service in holidays');
      const config = new Config(
        configExample(
          {
            storage: generateRamdonStorage(),
          },
          './fixtures/config/syncDoubleUplinksMetadata.yaml',
          __dirname
        )
      );
      const storage = new Storage(config);
      await storage.init(config);
      await expect(
        storage.syncUplinksMetadataNext(fooManifest.name, fooManifest, {
          retry: 0,
          timeout: {
            lookup: 100,
            connect: 50,
            secureConnect: 50,
            socket: 500,
            // send: 10000,
            response: 1000,
          },
        })
      ).rejects.toThrow('ETIMEDOUT');
    }, 10000);

    test('should handle one proxy fails', async () => {
      const fooManifest = generatePackageMetadata('foo', '8.0.0');
      nock('https://registry.verdaccio.org').get('/foo').replyWithError('service in holidays');
      const config = new Config(
        configExample(
          {
            storage: generateRamdonStorage(),
          },
          './fixtures/config/syncSingleUplinksMetadata.yaml',
          __dirname
        )
      );
      const storage = new Storage(config);
      await storage.init(config);
      await expect(
        storage.syncUplinksMetadataNext(fooManifest.name, fooManifest, {
          retry: 0,
        })
      ).rejects.toThrow(API_ERROR.NO_PACKAGE);
    });

    test('should handle one proxy reply 304', async () => {
      const fooManifest = generatePackageMetadata('foo', '8.0.0');
      nock('https://registry.verdaccio.org').get('/foo').reply(304);
      const config = new Config(
        configExample(
          {
            storage: generateRamdonStorage(),
          },
          './fixtures/config/syncSingleUplinksMetadata.yaml',
          __dirname
        )
      );
      const storage = new Storage(config);
      await storage.init(config);
      await expect(
        storage.syncUplinksMetadataNext(fooManifest.name, fooManifest, {
          retry: 0,
        })
      ).rejects.toThrow(API_ERROR.NOT_MODIFIED_NO_DATA);
    });

    test('should handle one proxy success', async () => {
      const fooManifest = generatePackageMetadata('foo', '8.0.0');
      nock('https://registry.verdaccio.org').get('/foo').reply(201, manifestFooRemoteNpmjs);
      const config = new Config(
        configExample(
          {
            storage: generateRamdonStorage(),
          },
          './fixtures/config/syncSingleUplinksMetadata.yaml',
          __dirname
        )
      );
      const storage = new Storage(config);
      await storage.init(config);

      const [response] = await storage.syncUplinksMetadataNext(fooManifest.name, fooManifest);
      expect(response.name).toEqual(fooManifest.name);
      expect(response[DIST_TAGS].latest).toEqual('8.0.0');
    });

    test('should handle no proxy found', async () => {
      const fooManifest = generatePackageMetadata('foo', '8.0.0');
      nock('https://registry.verdaccio.org').get('/foo').reply(201, manifestFooRemoteNpmjs);
      const config = new Config(
        configExample(
          {
            storage: generateRamdonStorage(),
          },
          './fixtures/config/syncNoUplinksMetadata.yaml',
          __dirname
        )
      );
      const storage = new Storage(config);
      await storage.init(config);

      const [response] = await storage.syncUplinksMetadataNext(fooManifest.name, fooManifest);
      expect(response.name).toEqual(fooManifest.name);
      expect(response[DIST_TAGS].latest).toEqual('8.0.0');
    });

    test('should handle disable uplinks via options.uplinksLook=false', async () => {
      const fooManifest = generatePackageMetadata('foo', '8.0.0');
      nock('https://registry.verdaccio.org').get('/foo').reply(201, manifestFooRemoteNpmjs);
      const config = new Config(
        configExample(
          {
            storage: generateRamdonStorage(),
          },
          './fixtures/config/syncSingleUplinksMetadata.yaml',
          __dirname
        )
      );
      const storage = new Storage(config);
      await storage.init(config);

      const [response] = await storage.syncUplinksMetadataNext(fooManifest.name, fooManifest, {
        uplinksLook: false,
      });
      expect(response.name).toEqual(fooManifest.name);
      expect(response[DIST_TAGS].latest).toEqual('8.0.0');
    });

    test('should handle one proxy success with clean metadata', async () => {
      nock('https://registry.verdaccio.org').get('/foo').reply(201, manifestFooRemoteNpmjs);
      const config = new Config(
        configExample(
          {
            storage: generateRamdonStorage(),
          },
          './fixtures/config/syncSingleUplinksMetadata.yaml',
          __dirname
        )
      );
      const storage = new Storage(config);
      await storage.init(config);

      // @ts-expect-error
      const [response] = await storage.syncUplinksMetadataNext(fooManifest.name, {});
      expect(response.name).toEqual(fooManifest.name);
      // the latest from the remote manifest
      expect(response[DIST_TAGS].latest).toEqual('0.0.7');
    });

    // test('should handle all proxy fails', async () => {
    //   const fooManifest = generatePackageMetadata('foo', '8.0.0');
    //   nock('https://registry.npmjs.org').get('/foo').reply(201, manifestFooRemoteNpmjs);
    //   nock('https://registry.verdaccio.org').get('/foo').replyWithError('service in holidays');
    //   const config = new Config(
    //     configExample(
    //       {
    //         storage: generateRamdonStorage(),
    //       },
    //       './fixtures/config/syncUplinksMetadata.yaml',
    //       __dirname
    //     )
    //   );
    //   const storage = new Storage(config);
    //   await storage.init(config);
    //   await storage.syncUplinksMetadataNext(fooManifest.name, fooManifest, { retry: 0 });
    // });

    // test('should secondary proxy override primary proxy', async () => {
    //   const fooManifest = generatePackageMetadata('foo', '8.0.0');
    //   nock('https://registry.npmjs.org').get('/foo').reply(201, manifestFooRemoteNpmjs);
    //   nock('https://registry.verdaccio.org').get('/foo').replyWithError('service in holidays');
    //   const config = new Config(
    //     configExample(
    //       {
    //         storage: generateRamdonStorage(),
    //       },
    //       './fixtures/config/syncUplinksMetadata.yaml',
    //       __dirname
    //     )
    //   );
    //   const storage = new Storage(config);
    //   await storage.init(config);
    //   await storage.syncUplinksMetadataNext(fooManifest.name, fooManifest, { retry: 0 });
    // });
  });

  // TODO: getPackageNext should replace getPackage eventually
  describe('get packages getPackageNext()', () => {
    describe('with uplinks', () => {
      test('should get 201 and merge from uplink', async () => {
        nock(domain).get('/foo').reply(201, fooManifest);
        const config = new Config(
          configExample({
            storage: generateRamdonStorage(),
          })
        );
        const req = httpMocks.createRequest({
          method: 'GET',
          connection: { remoteAddress: fakeHost },
          headers: {
            host: fakeHost,
            [HEADERS.FORWARDED_PROTO]: 'http',
          },
          url: '/',
        });
        const storage = new Storage(config);
        await storage.init(config);
        await expect(
          storage.getPackageByOptions({
            name: 'foo',
            uplinksLook: true,
            req,
            requestOptions: {
              headers: req.headers as any,
              protocol: req.protocol,
              host: req.get('host'),
            },
          })
        ).resolves.toEqual(expect.objectContaining({ name: 'foo' }));
      });

      test('should get 201 and merge from uplink with version', async () => {
        nock(domain).get('/foo').reply(201, fooManifest);
        const config = new Config(
          configExample({
            storage: generateRamdonStorage(),
          })
        );
        const req = httpMocks.createRequest({
          method: 'GET',
          connection: { remoteAddress: fakeHost },
          headers: {
            host: fakeHost,
            [HEADERS.FORWARDED_PROTO]: 'http',
          },
          url: '/',
        });
        const storage = new Storage(config);
        await storage.init(config);
        await expect(
          storage.getPackageByOptions({
            name: 'foo',
            version: '1.0.0',
            uplinksLook: true,
            req,
            requestOptions: {
              headers: req.headers as any,
              protocol: req.protocol,
              host: req.get('host'),
            },
          })
        ).resolves.toEqual(expect.objectContaining({ name: 'foo' }));
      });

      test('should get 201 and merge from uplink with dist-tag', async () => {
        nock(domain).get('/foo').reply(201, fooManifest);
        const config = new Config(
          configExample({
            storage: generateRamdonStorage(),
          })
        );
        const req = httpMocks.createRequest({
          method: 'GET',
          connection: { remoteAddress: fakeHost },
          headers: {
            host: fakeHost,
            [HEADERS.FORWARDED_PROTO]: 'http',
          },
          url: '/',
        });
        const storage = new Storage(config);
        await storage.init(config);
        await expect(
          storage.getPackageByOptions({
            name: 'foo',
            version: 'latest',
            uplinksLook: true,
            req,
            requestOptions: {
              headers: req.headers as any,
              protocol: req.protocol,
              host: req.get('host'),
            },
          })
        ).resolves.toEqual(expect.objectContaining({ name: 'foo' }));
      });

      test('should get 404 for version does not exist', async () => {
        nock(domain).get('/foo').reply(201, fooManifest);
        const config = new Config(
          configExample({
            storage: generateRamdonStorage(),
          })
        );
        const req = httpMocks.createRequest({
          method: 'GET',
          connection: { remoteAddress: fakeHost },
          headers: {
            host: fakeHost,
            [HEADERS.FORWARDED_PROTO]: 'http',
          },
          url: '/',
        });
        const storage = new Storage(config);
        await storage.init(config);
        await expect(
          storage.getPackageByOptions({
            name: 'foo',
            version: '1.0.0-does-not-exist',
            uplinksLook: true,
            req,
            requestOptions: {
              headers: req.headers as any,
              protocol: req.protocol,
              host: req.get('host'),
            },
          })
        ).rejects.toThrow(
          errorUtils.getNotFound("this version doesn't exist: 1.0.0-does-not-exist")
        );
      });

      test('should get 404', async () => {
        nock(domain).get('/foo2').reply(404);
        const config = new Config(
          configExample({
            storage: generateRamdonStorage(),
          })
        );
        const req = httpMocks.createRequest({
          method: 'GET',
          connection: { remoteAddress: fakeHost },
          headers: {
            host: fakeHost,
            [HEADERS.FORWARDED_PROTO]: 'http',
          },
          url: '/',
        });
        const storage = new Storage(config);
        await storage.init(config);
        await expect(
          storage.getPackageByOptions({
            name: 'foo2',
            uplinksLook: true,
            req,
            requestOptions: {
              headers: req.headers as any,
              protocol: req.protocol,
              host: req.get('host'),
            },
          })
        ).rejects.toThrow(errorUtils.getNotFound());
      });

      test('should get ETIMEDOUT with uplink', async () => {
        nock(domain).get('/foo2').replyWithError({
          code: 'ETIMEDOUT',
          errno: 'ETIMEDOUT',
        });
        const config = new Config(
          configExample({
            storage: generateRamdonStorage(),
          })
        );
        const req = httpMocks.createRequest({
          method: 'GET',
          connection: { remoteAddress: fakeHost },
          headers: {
            host: fakeHost,
            [HEADERS.FORWARDED_PROTO]: 'http',
          },
          url: '/',
        });
        const storage = new Storage(config);
        await storage.init(config);
        await expect(
          storage.getPackageByOptions({
            name: 'foo2',
            uplinksLook: true,
            req,
            requestOptions: {
              headers: req.headers as any,
              protocol: req.protocol,
              host: req.get('host'),
            },
          })
        ).rejects.toThrow(errorUtils.getServiceUnavailable());
      });
    });
  });
});
