# Gnocchi devstack plugin
# Install and start **Gnocchi** service

# To enable Gnocchi service, add the following to localrc:
#
#   enable_plugin gnocchi https://github.com/gnocchixyz/gnocchi master
#
# This will turn on both gnocchi-api and gnocchi-metricd services.
# If you don't want one of those (you do) you can use the
# disable_service command in local.conf.

# Dependencies:
#
# - functions
# - ``functions``
# - ``DEST``, ``STACK_USER`` must be defined
# - ``APACHE_NAME`` for wsgi
# - ``SERVICE_{TENANT_NAME|PASSWORD}`` must be defined
# - ``SERVICE_HOST``
# - ``OS_AUTH_URL``, ``KEYSTONE_SERVICE_URI`` for auth in api

# stack.sh
# ---------
# - install_gnocchi
# - configure_gnocchi
# - init_gnocchi
# - start_gnocchi
# - stop_gnocchi
# - cleanup_gnocchi

# Save trace setting
XTRACE=$(set +o | grep xtrace)
set -o xtrace


if [ -z "$GNOCCHI_DEPLOY" ]; then
    # Default
    GNOCCHI_DEPLOY=simple

    # Fallback to common wsgi devstack configuration
    if [ "$ENABLE_HTTPD_MOD_WSGI_SERVICES" == "True" ]; then
        GNOCCHI_DEPLOY=mod_wsgi
    fi
fi

# Functions
# ---------

# Test if any Gnocchi services are enabled
# is_gnocchi_enabled
function is_gnocchi_enabled {
    [[ ,${ENABLED_SERVICES} =~ ,"gnocchi-" ]] && return 0
    return 1
}

# Test if a Ceph services are enabled
# _is_ceph_enabled
function _is_ceph_enabled {
    type is_ceph_enabled_for_service >/dev/null 2>&1 && return 0
    return 1
}

# create_gnocchi_accounts() - Set up common required gnocchi accounts

# Project              User            Roles
# -------------------------------------------------------------------------
# $SERVICE_TENANT_NAME  gnocchi        service
# gnocchi_swift         gnocchi_swift  ResellerAdmin  (if Swift is enabled)
function create_gnocchi_accounts {
    # Gnocchi
    if [ "$GNOCCHI_USE_KEYSTONE" == "True" ] && is_service_enabled gnocchi-api ; then
        # At this time, the /etc/openstack/clouds.yaml is available,
        # we could leverage that by setting OS_CLOUD
        OLD_OS_CLOUD=$OS_CLOUD
        export OS_CLOUD='devstack-admin'

        create_service_user "gnocchi"

        local gnocchi_service=$(get_or_create_service "gnocchi" \
            "metric" "OpenStack Metric Service")
        get_or_create_endpoint $gnocchi_service \
            "$REGION_NAME" \
            "$(gnocchi_service_url)" \
            "$(gnocchi_service_url)" \
            "$(gnocchi_service_url)"

        if is_service_enabled swift && [[ "$GNOCCHI_STORAGE_BACKEND" = 'swift' ]] ; then
            get_or_create_project "gnocchi_swift" default
            local gnocchi_swift_user=$(get_or_create_user "gnocchi_swift" \
                "$SERVICE_PASSWORD" default "gnocchi_swift@example.com")
            get_or_add_user_project_role "ResellerAdmin" $gnocchi_swift_user "gnocchi_swift"
        fi

        export OS_CLOUD=$OLD_OS_CLOUD
    fi
}

# return the service url for gnocchi
function gnocchi_service_url {
    if [[ -n $GNOCCHI_SERVICE_PORT ]]; then
        echo "$GNOCCHI_SERVICE_PROTOCOL://$GNOCCHI_SERVICE_HOST:$GNOCCHI_SERVICE_PORT"
    else
        echo "$GNOCCHI_SERVICE_PROTOCOL://$GNOCCHI_SERVICE_HOST$GNOCCHI_SERVICE_PREFIX"
    fi
}

# install redis
# NOTE(chdent): We shouldn't rely on ceilometer being present so cannot
# use its install_redis. There are enough packages now using redis
# that there should probably be something devstack itself for
# installing it.
function _gnocchi_install_redis {
    if is_ubuntu; then
        install_package redis-server
        restart_service redis-server
    else
        # This will fail (correctly) where a redis package is unavailable
        install_package redis
        if is_suse; then
            # opensuse intsall multi-instance version of redis
            # and admin is expected to install the required conf
            cp /etc/redis/default.conf.example /etc/redis/default.conf
            restart_service redis@default
        else
            restart_service redis
        fi
    fi

    pip_install_gr redis
}

function _gnocchi_install_grafana {
    if is_ubuntu; then
        local file=$(mktemp /tmp/grafanapkg-XXXXX)
        wget -O "$file" "$GRAFANA_DEB_PKG"
        sudo dpkg -i "$file"
        rm $file
    elif is_fedora; then
        sudo yum install "$GRAFANA_RPM_PKG"
    fi
    sudo -u grafana mkdir -p /var/lib/grafana/plugins
    sudo rm -rf /var/lib/grafana/plugins/grafana-gnocchi-datasource
    if [ ! "$GRAFANA_PLUGIN_VERSION" ]; then
        sudo grafana-cli plugins install gnocchixyz-gnocchi-datasource
    elif [ "$GRAFANA_PLUGIN_VERSION" != "git" ]; then
        tmpfile=/tmp/gnocchixyz-gnocchi-datasource-${GRAFANA_PLUGIN_VERSION}.tar.gz
        wget https://github.com/gnocchixyz/grafana-gnocchi-datasource/releases/download/${GRAFANA_PLUGIN_VERSION}/gnocchixyz-gnocchi-datasource-${GRAFANA_PLUGIN_VERSION}.tar.gz -O $tmpfile
        sudo -u grafana tar -xzf $tmpfile -C /var/lib/grafana/plugins
        rm -f $file
    else
        git_clone ${GRAFANA_PLUGINS_REPO} ${GRAFANA_PLUGINS_DIR}
        sudo ln -sf ${GRAFANA_PLUGINS_DIR}/dist  /var/lib/grafana/plugins/grafana-gnocchi-datasource
        # NOTE(sileht): This is long and have chance to fail, thx nodejs/npm
        (cd /var/lib/grafana/plugins/grafana-gnocchi-datasource && npm install && ./run-tests.sh) || true
    fi
    sudo service grafana-server restart
}

function _cleanup_gnocchi_apache_wsgi {
    sudo rm -f $(apache_site_config_for gnocchi)
}

# _config_gnocchi_apache_wsgi() - Set WSGI config files of Gnocchi
function _config_gnocchi_apache_wsgi {
    local gnocchi_apache_conf=$(apache_site_config_for gnocchi)
    local venv_path=""
    local script_name=$GNOCCHI_SERVICE_PREFIX

    if [[ ${USE_VENV} = True ]]; then
        venv_path="python-path=${PROJECT_VENV["gnocchi"]}/lib/$(python_version)/site-packages"
    fi

    # Only run the API on a custom PORT if it has been specifically
    # asked for.
    if [[ -n $GNOCCHI_SERVICE_PORT ]]; then
        sudo cp $GNOCCHI_DIR/devstack/apache-ported-gnocchi.template $gnocchi_apache_conf
        sudo sed -e "
            s|%GNOCCHI_PORT%|$GNOCCHI_SERVICE_PORT|g;
        " -i $gnocchi_apache_conf
    else
        sudo cp $GNOCCHI_DIR/devstack/apache-gnocchi.template $gnocchi_apache_conf
        sudo sed -e "
            s|%SCRIPT_NAME%|$script_name|g;
        " -i $gnocchi_apache_conf
    fi
    sudo sed -e "
            s|%APACHE_NAME%|$APACHE_NAME|g;
            s|%WSGI%|$GNOCCHI_BIN_DIR/gnocchi-api|g;
            s|%USER%|$STACK_USER|g
            s|%APIWORKERS%|$API_WORKERS|g
            s|%VIRTUALENV%|$venv_path|g
        " -i $gnocchi_apache_conf
}



# cleanup_gnocchi() - Remove residual data files, anything left over from previous
# runs that a clean run would need to clean up
function cleanup_gnocchi {
    if [ "$GNOCCHI_DEPLOY" == "mod_wsgi" ]; then
        _cleanup_gnocchi_apache_wsgi
    fi
}

# configure_gnocchi() - Set config files, create data dirs, etc
function configure_gnocchi {
    [ ! -d $GNOCCHI_DATA_DIR ] && sudo mkdir -m 755 -p $GNOCCHI_DATA_DIR
    sudo chown $STACK_USER $GNOCCHI_DATA_DIR

    # Configure logging
    iniset $GNOCCHI_CONF DEFAULT debug "$ENABLE_DEBUG_LOG_LEVEL"
    iniset $GNOCCHI_CONF metricd metric_processing_delay "$GNOCCHI_METRICD_PROCESSING_DELAY"

    # Set up logging
    if [ "$SYSLOG" != "False" ]; then
        iniset $GNOCCHI_CONF DEFAULT use_syslog "True"
    fi

    # Format logging
    if [ "$LOG_COLOR" == "True" ] && [ "$SYSLOG" == "False" ] && [ "$GNOCCHI_DEPLOY" != "mod_wsgi" ]; then
        setup_colorized_logging $GNOCCHI_CONF DEFAULT
    fi

    if [ -n "$GNOCCHI_COORDINATOR_URL" ]; then
        iniset $GNOCCHI_CONF DEFAULT coordination_url "$GNOCCHI_COORDINATOR_URL"
    fi

    if is_service_enabled gnocchi-statsd ; then
        iniset $GNOCCHI_CONF statsd resource_id $GNOCCHI_STATSD_RESOURCE_ID
        iniset $GNOCCHI_CONF statsd creator $GNOCCHI_STATSD_CREATOR
    fi

    # Configure the storage driver
    if _is_ceph_enabled && [[ "$GNOCCHI_STORAGE_BACKEND" = 'ceph' ]] ; then
        iniset $GNOCCHI_CONF storage driver ceph
        iniset $GNOCCHI_CONF storage ceph_username ${GNOCCHI_CEPH_USER}
        iniset $GNOCCHI_CONF storage ceph_secret $(awk '/key/{print $3}' ${CEPH_CONF_DIR}/ceph.client.${GNOCCHI_CEPH_USER}.keyring)
    elif is_service_enabled swift && [[ "$GNOCCHI_STORAGE_BACKEND" = 'swift' ]] ; then
        iniset $GNOCCHI_CONF storage driver swift
        iniset $GNOCCHI_CONF storage swift_user gnocchi_swift
        iniset $GNOCCHI_CONF storage swift_key $SERVICE_PASSWORD
        iniset $GNOCCHI_CONF storage swift_project_name "gnocchi_swift"
        iniset $GNOCCHI_CONF storage swift_auth_version 3
        iniset $GNOCCHI_CONF storage swift_authurl $KEYSTONE_SERVICE_URI_V3
    elif [[ "$GNOCCHI_STORAGE_BACKEND" = 'file' ]] ; then
        iniset $GNOCCHI_CONF storage driver file
        iniset $GNOCCHI_CONF storage file_basepath $GNOCCHI_DATA_DIR/
    elif [[ "$GNOCCHI_STORAGE_BACKEND" = 'redis' ]] ; then
        iniset $GNOCCHI_CONF storage driver redis
        iniset $GNOCCHI_CONF storage redis_url $GNOCCHI_REDIS_URL
    else
        echo "ERROR: could not configure storage driver"
        exit 1
    fi

    if [ "$GNOCCHI_USE_KEYSTONE" == "True" ] ; then
        # Configure auth token middleware
        configure_auth_token_middleware $GNOCCHI_CONF gnocchi $GNOCCHI_AUTH_CACHE_DIR
        iniset $GNOCCHI_CONF api auth_mode keystone
        if is_service_enabled gnocchi-grafana; then
            iniset $GNOCCHI_CONF cors allowed_origin ${GRAFANA_URL}
        fi
    else
        inidelete $GNOCCHI_CONF api auth_mode
    fi

    # Configure the indexer database
    iniset $GNOCCHI_CONF indexer url `database_connection_url gnocchi`

    if [ "$GNOCCHI_DEPLOY" == "mod_wsgi" ]; then
        _config_gnocchi_apache_wsgi
    elif [ "$GNOCCHI_DEPLOY" == "uwsgi" ]; then
        # iniset creates these files when it's called if they don't exist.
        GNOCCHI_UWSGI_FILE=$GNOCCHI_CONF_DIR/uwsgi.ini

        rm -f "$GNOCCHI_UWSGI_FILE"

        iniset "$GNOCCHI_UWSGI_FILE" uwsgi http $GNOCCHI_SERVICE_HOST:$GNOCCHI_SERVICE_PORT
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi wsgi-file "$GNOCCHI_BIN_DIR/gnocchi-api"
        # This is running standalone
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi master true
        # Set die-on-term & exit-on-reload so that uwsgi shuts down
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi die-on-term true
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi exit-on-reload true
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi threads 32
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi processes $API_WORKERS
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi enable-threads true
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi plugins python
        # uwsgi recommends this to prevent thundering herd on accept.
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi thunder-lock true
        # Override the default size for headers from the 4k default.
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi buffer-size 65535
        # Make sure the client doesn't try to re-use the connection.
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi add-header "Connection: close"
        # Don't share rados resources and python-requests globals between processes
        iniset "$GNOCCHI_UWSGI_FILE" uwsgi lazy-apps true
    fi
}

# configure_keystone_for_gnocchi() - Configure Keystone needs for Gnocchi
function configure_keystone_for_gnocchi {
    if [ "$GNOCCHI_USE_KEYSTONE" == "True" ] ; then
        if is_service_enabled gnocchi-grafana; then
            # NOTE(sileht): keystone configuration have to be set before uwsgi
            # is started
            iniset $KEYSTONE_CONF cors allowed_origin ${GRAFANA_URL}
        fi
    fi
}

# configure_ceph_gnocchi() - gnocchi config needs to come after gnocchi is set up
function configure_ceph_gnocchi {
    # Configure gnocchi service options, ceph pool, ceph user and ceph key
    sudo ceph -c ${CEPH_CONF_FILE} osd pool create ${GNOCCHI_CEPH_POOL} ${GNOCCHI_CEPH_POOL_PG} ${GNOCCHI_CEPH_POOL_PGP}
    sudo ceph -c ${CEPH_CONF_FILE} osd pool set ${GNOCCHI_CEPH_POOL} size ${CEPH_REPLICAS}
    if [[ $CEPH_REPLICAS -ne 1 ]]; then
        sudo ceph -c ${CEPH_CONF_FILE} osd pool set ${GNOCCHI_CEPH_POOL} crush_ruleset ${RULE_ID}

    fi
    sudo ceph -c ${CEPH_CONF_FILE} auth get-or-create client.${GNOCCHI_CEPH_USER} mon "allow r" osd "allow rwx pool=${GNOCCHI_CEPH_POOL}" | sudo tee ${CEPH_CONF_DIR}/ceph.client.${GNOCCHI_CEPH_USER}.keyring
    sudo chown ${STACK_USER}:$(id -g -n $whoami) ${CEPH_CONF_DIR}/ceph.client.${GNOCCHI_CEPH_USER}.keyring
}


# init_gnocchi() - Initialize etc.
function init_gnocchi {
    # Create cache dir
    sudo mkdir -p $GNOCCHI_AUTH_CACHE_DIR
    sudo chown $STACK_USER $GNOCCHI_AUTH_CACHE_DIR
    rm -f $GNOCCHI_AUTH_CACHE_DIR/*

    if is_service_enabled mysql postgresql; then
        recreate_database gnocchi
    fi
    $GNOCCHI_BIN_DIR/gnocchi-upgrade
}

function preinstall_gnocchi {
    if is_ubuntu; then
        # libpq-dev is needed to build psycopg2
        # uuid-runtime is needed to use the uuidgen command
        install_package libpq-dev uuid-runtime
    else
        install_package postgresql-devel
    fi
    if [[ "$GNOCCHI_STORAGE_BACKEND" = 'ceph' ]] ; then
            install_package cython
            install_package librados-dev
    fi
}

# install_gnocchi() - Collect source and prepare
function install_gnocchi {
    if [[ "$GNOCCHI_STORAGE_BACKEND" = 'redis' ]] || [[ "${GNOCCHI_COORDINATOR_URL%%:*}" == "redis" ]]; then
        _gnocchi_install_redis
    fi

    if [[ "$GNOCCHI_STORAGE_BACKEND" = 'ceph' ]] ; then
        pip_install cradox
    fi

    if is_service_enabled gnocchi-grafana
    then
        _gnocchi_install_grafana
    fi

    [ "$GNOCCHI_USE_KEYSTONE" == "True" ] && EXTRA_FLAVOR=,keystone

    # We don't use setup_package because we don't follow openstack/requirements
    sudo -H pip install -e "$GNOCCHI_DIR"[test,$GNOCCHI_STORAGE_BACKEND,${DATABASE_TYPE}${EXTRA_FLAVOR}]

    if [ "$GNOCCHI_DEPLOY" == "mod_wsgi" ]; then
        install_apache_wsgi
    elif [ "$GNOCCHI_DEPLOY" == "uwsgi" ]; then
        pip_install uwsgi
    fi

    # Create configuration directory
    [ ! -d $GNOCCHI_CONF_DIR ] && sudo mkdir -m 755 -p $GNOCCHI_CONF_DIR
    sudo chown $STACK_USER $GNOCCHI_CONF_DIR
}

# start_gnocchi() - Start running processes, including screen
function start_gnocchi {

    if [ "$GNOCCHI_DEPLOY" == "mod_wsgi" ]; then
        enable_apache_site gnocchi
        restart_apache_server
        if [[ -n $GNOCCHI_SERVICE_PORT ]]; then
            tail_log gnocchi /var/log/$APACHE_NAME/gnocchi.log
            tail_log gnocchi-api /var/log/$APACHE_NAME/gnocchi-access.log
        else
            # NOTE(chdent): At the moment this is very noisy as it
            # will tail the entire apache logs, not just the gnocchi
            # parts. If you don't like this either USE_SCREEN=False
            # or set GNOCCHI_SERVICE_PORT.
            tail_log gnocchi /var/log/$APACHE_NAME/error[_\.]log
            tail_log gnocchi-api /var/log/$APACHE_NAME/access[_\.]log
        fi
    elif [ "$GNOCCHI_DEPLOY" == "uwsgi" ]; then
        run_process gnocchi-api "$GNOCCHI_BIN_DIR/uwsgi $GNOCCHI_UWSGI_FILE"
    else
        run_process gnocchi-api "$GNOCCHI_BIN_DIR/gnocchi-api --port $GNOCCHI_SERVICE_PORT"
    fi
    # only die on API if it was actually intended to be turned on
    if is_service_enabled gnocchi-api; then

        echo "Waiting for gnocchi-api to start..."
        if ! timeout $SERVICE_TIMEOUT sh -c "while ! curl -v --max-time 5 --noproxy '*' -s $(gnocchi_service_url)/v1/resource/generic ; do sleep 1; done"; then
            die $LINENO "gnocchi-api did not start"
        fi
    fi

    # run metricd last so we are properly waiting for swift and friends
    run_process gnocchi-metricd "$GNOCCHI_BIN_DIR/gnocchi-metricd -d --config-file $GNOCCHI_CONF"
    run_process gnocchi-statsd "$GNOCCHI_BIN_DIR/gnocchi-statsd -d --config-file $GNOCCHI_CONF"
}

# stop_gnocchi() - Stop running processes
function stop_gnocchi {
    if [ "$GNOCCHI_DEPLOY" == "mod_wsgi" ]; then
        disable_apache_site gnocchi
        restart_apache_server
    fi
    # Kill the gnocchi screen windows
    for serv in gnocchi-api gnocchi-metricd gnocchi-statsd; do
        stop_process $serv
    done
}

if is_service_enabled gnocchi-api; then
    if [[ "$1" == "stack" && "$2" == "pre-install" ]]; then
        echo_summary "Configuring system services for Gnocchi"
        preinstall_gnocchi
    elif [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo_summary "Installing Gnocchi"
        stack_install_service gnocchi
        configure_keystone_for_gnocchi
    elif [[ "$1" == "stack" && "$2" == "post-config" ]]; then
        echo_summary "Configuring Gnocchi"
        if _is_ceph_enabled && [[ "$GNOCCHI_STORAGE_BACKEND" = 'ceph' ]] ; then
            echo_summary "Configuring Gnocchi for Ceph"
            configure_ceph_gnocchi
        fi
        configure_gnocchi
        create_gnocchi_accounts
    elif [[ "$1" == "stack" && "$2" == "extra" ]]; then
        echo_summary "Initializing Gnocchi"
        init_gnocchi
        start_gnocchi
    fi

    if [[ "$1" == "unstack" ]]; then
        echo_summary "Stopping Gnocchi"
        stop_gnocchi
    fi

    if [[ "$1" == "clean" ]]; then
        cleanup_gnocchi
    fi
fi

# Restore xtrace
$XTRACE

# Tell emacs to use shell-script-mode
## Local variables:
## mode: shell-script
## End:
