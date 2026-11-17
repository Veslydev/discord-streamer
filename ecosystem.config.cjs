module.exports = {
  apps: [
    {
      name: "discord-streamer",
      cwd: __dirname,
      script: "src/index.js",
      interpreter: "bun",
      exec_mode: "fork",
      instances: 1,
      autorestart: true,
      restart_delay: 5000,
      min_uptime: "30s",
      max_restarts: 10,
      stop_exit_codes: [78],
      time: true,
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
