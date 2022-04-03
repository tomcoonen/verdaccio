/* eslint-disable @typescript-eslint/no-unused-vars */
import buildDebug from 'debug';
import _ from 'lodash';

import { logger } from '@verdaccio/logger';
import { IProxy, ISyncUplinksOptions, ProxyList } from '@verdaccio/proxy';
import {
  Callback,
  CallbackAction,
  Config,
  DistFile,
  GenericBody,
  IReadTarball,
  IUploadTarball,
  Logger,
  Manifest,
  MergeTags,
  Package,
  StringValue,
  Token,
  TokenFilter,
  Version,
} from '@verdaccio/types';

import { LocalStorage } from './local-storage';
import { SearchManager } from './search';
import { IGetPackageOptions, IGetPackageOptionsNext, IPluginFilters, ISyncUplinks } from './type';
import {
  setupUpLinks,
  updateVersionsHiddenUpLink,
  updateVersionsHiddenUpLinkNext,
} from './uplink-util';

const debug = buildDebug('verdaccio:storage:abstract');

class AbstractStorage {
  public localStorage: LocalStorage;
  public searchManager: SearchManager | null;
  public filters: IPluginFilters;
  public readonly config: Config;
  public readonly logger: Logger;
  public readonly uplinks: ProxyList;
  public constructor(config: Config) {
    this.config = config;
    this.uplinks = setupUpLinks(config);
    this.logger = logger.child({ module: 'storage' });
    this.filters = [];
    // @ts-ignore
    this.localStorage = null;
    this.searchManager = null;
  }

  /**
   * Initialize the storage asyncronously.
   * @param config Config
   * @param filters IPluginFilters
   * @returns Storage instance
   */
  public async init(config: Config, filters: IPluginFilters = []): Promise<void> {
    if (this.localStorage === null) {
      this.filters = filters || [];
      debug('filters available %o', filters);
      this.localStorage = new LocalStorage(this.config, logger);
      await this.localStorage.init();
      debug('local init storage initialized');
      await this.localStorage.getSecret(config);
      debug('local storage secret initialized');
      this.searchManager = new SearchManager(this.uplinks, this.localStorage);
    } else {
      debug('storage has been already initialized');
    }
    return;
  }

  public _isAllowPublishOffline(): boolean {
    return (
      typeof this.config.publish !== 'undefined' &&
      _.isBoolean(this.config.publish.allow_offline) &&
      this.config.publish.allow_offline
    );
  }

  public readTokens(filter: TokenFilter): Promise<Token[]> {
    return this.localStorage.readTokens(filter);
  }

  public saveToken(token: Token): Promise<void> {
    return this.localStorage.saveToken(token);
  }

  public deleteToken(user: string, tokenKey: string): Promise<any> {
    return this.localStorage.deleteToken(user, tokenKey);
  }

  /**
   * Tags a package version with a provided tag
     Used storages: local (write)
   */
  public mergeTags(name: string, tagHash: MergeTags, callback: CallbackAction): void {
    debug('merge tags for package %o tags %o', name, tagHash);
    this.localStorage.mergeTags(name, tagHash, callback);
  }

  /**
   * Apply filters to manifest.
   * @param manifest
   * @returns
   */
  public async applyFilters(manifest: Manifest): Promise<[Manifest, any]> {
    if (this.filters.length === 0) {
      return [manifest, []];
    }

    let filterPluginErrors: any[] = [];
    let filteredManifest = { ...manifest };
    for (const filter of this.filters) {
      // These filters can assume it's save to modify packageJsonLocal
      // and return it directly for
      // performance (i.e. need not be pure)
      try {
        filteredManifest = await filter.filter_metadata(manifest);
      } catch (err: any) {
        this.logger.error({ err: err.message }, 'filter has failed @{err}');
        filterPluginErrors.push(err);
      }
    }
    return [filteredManifest, filterPluginErrors];
  }
}

export default AbstractStorage;
