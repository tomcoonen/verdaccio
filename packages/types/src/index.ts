import { NextFunction, Request, Response } from 'express';

import {
  Author,
  Callback,
  Config,
  IBasicStorage,
  IPluginStorage,
  IPluginStorageFilter,
  IReadTarball,
  IStorageManager,
  ITokenActions,
  Logger,
  Package,
  PackageAccess,
  RemoteUser,
  Token,
  TokenFilter,
  UpLinkConf,
  StringValue as verdaccio$StringValue,
} from '@verdaccio/types';

export type StringValue = verdaccio$StringValue;

// legacy should be removed in long term
export interface LegacyPackageList {
  [key: string]: PackageAccessAddOn;
}

export type PackageAccessAddOn = PackageAccess & {
  // FIXME: should be published on @verdaccio/types
  unpublish?: string[];
};

export type MatchedPackage = PackageAccess | void;

export type JWTPayload = RemoteUser & {
  password?: string;
};

export interface AESPayload {
  user: string;
  password: string;
}

export interface AuthTokenHeader {
  scheme: string;
  token: string;
}

export type BasicPayload = AESPayload | void;
export type AuthMiddlewarePayload = RemoteUser | BasicPayload;

export interface ProxyList {
  [key: string]: IProxy;
}

export interface CookieSessionToken {
  expires: Date;
}

export interface Utils {
  ErrorCode: any;
  getLatestVersion: Callback;
  isObject: (value: any) => boolean;
  validate_name: (value: any) => boolean;
  tag_version: (value: any, version: string, tag: string) => void;
  normalizeDistTags: (pkg: Package) => void;
  semverSort: (keys: string[]) => string[];
}

export interface Profile {
  tfa: boolean;
  name: string;
  email: string;
  email_verified: string;
  created: string;
  updated: string;
  cidr_whitelist: any;
  fullname: string;
}

export type $RequestExtend = Request & { remote_user?: any; log: Logger };
export type $ResponseExtend = Response & { cookies?: any };
export type $NextFunctionVer = NextFunction & any;
export type $SidebarPackage = Package & { latest: any };

// FIXME: This prop should be on @verdaccio/types
export type UpLinkConfLocal = UpLinkConf & {
  no_proxy?: string;
};
// @deprecated use @verdaccio/proxy
export interface IProxy {
  config: UpLinkConfLocal;
  failed_requests: number;
  userAgent: string;
  ca?: string | void;
  logger: Logger;
  server_id: string;
  url: any;
  maxage: number;
  timeout: number;
  max_fails: number;
  fail_timeout: number;
  upname: string;
  fetchTarball(url: string): IReadTarball;
  isUplinkValid(url: string): boolean;
  search(options: any);
  getRemoteMetadata(name: string, options: any, callback: Callback): void;
}

export interface IStorage extends IBasicStorage<Config>, ITokenActions {
  config: Config;
  storagePlugin: IPluginStorage<Config>;
  logger: Logger;
}

export interface IGetPackageOptions {
  callback: Callback;
  name: string;
  keepUpLinkData: boolean;
  uplinksLook: boolean;
  req: any;
}
// @deprecated use @verdaccio/proxy
export interface ISyncUplinks {
  uplinksLook?: boolean;
  etag?: string;
  req?: Request;
}

export type IPluginFilters = IPluginStorageFilter<Config>[];

// @deprecated use @verdaccio/storage
export interface Storage extends IStorageManager<Config>, ITokenActions {
  config: Config;
  localStorage: IStorage | null;
  filters: IPluginFilters;
  uplinks: ProxyList;
  init(config: Config, filters: IPluginFilters): Promise<string>;
  saveToken(token: Token): Promise<any>;
  deleteToken(user: string, tokenKey: string): Promise<any>;
  readTokens(filter: TokenFilter): Promise<Token[]>;
  _syncUplinksMetadata(name: string, packageInfo: Package, options: any, callback: Callback): void;
}

/**
 * @property { string | number | Styles }  [ruleOrSelector]
 */
export interface Styles {
  [ruleOrSelector: string]: string | number | Styles;
}

export type AuthorAvatar = Author & { avatar?: string };

export type SearchResultWeb = {
  name: string;
  version: string;
};
