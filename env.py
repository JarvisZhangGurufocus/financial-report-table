def env():
  env_vars = []
  with open('.env') as f:
    for line in f:
      if line.startswith('#'):
        continue
      key, value = line.strip().split('=', 1)
      env_vars.append({'name': key, 'value': value})
  return env_vars