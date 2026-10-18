const path = require('node:path');

const root = __dirname;
const accountId = process.env.KOTAK_ACCOUNT_ID || 'default';
const groupId = process.env.KOTAK_GROUP_ID || `kotak_executor_${accountId}`;

module.exports = {
  apps: [
    {
      name: 'kotak-telegram-producer',
      cwd: path.join(root, 'producer'),
      script: 'main.py',
      interpreter: path.join(root, 'producer', '.venv', 'bin', 'python'),
      autorestart: true,
      restart_delay: 5000,
      exp_backoff_restart_delay: 1000,
      env: { PYTHONUNBUFFERED: '1' },
    },
    {
      name: 'kotak-telegram-consumer',
      cwd: path.join(root, 'consumer'),
      script: 'main.py',
      interpreter: path.join(root, 'consumer', '.venv', 'bin', 'python'),
      args: ['--account-id', accountId, '--group-id', groupId],
      autorestart: true,
      restart_delay: 5000,
      exp_backoff_restart_delay: 1000,
      env: { PYTHONUNBUFFERED: '1' },
    },
  ],
};
