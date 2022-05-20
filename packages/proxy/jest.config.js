const config = require('../../jest/config');

module.exports = Object.assign({}, config, {
  collectCoverage: true,
  coverageThreshold: {
    global: {
      branches: 75,
      functions: 90,
      lines: 84,
      statements: 84,
    },
  },
});
