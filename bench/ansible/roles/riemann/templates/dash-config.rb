# riemann dashboard configuration

set :bind, '{{ riemann_dash_bind_address }}'
set :port, {{ riemann_dash_port }}

config[:ws_config] = '/data/riemann/dashboards.json'
