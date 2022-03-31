import fs from 'fs-extra';
import os from 'os';
import path from 'path';

export function createTempFolder(prefix: string) {
  return fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), prefix));
}
