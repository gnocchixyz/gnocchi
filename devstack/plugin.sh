# Gnocchi devstack plugin
# Install and start **Gnocchi** service

# To enable Gnocchi service, add the following to localrc:
#
#   enable_plugin gnocchi https://github.com/openstack/gnocchi master
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
# - ``CEILOMETER_CONF`` for ceilometer dispatcher configuration

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


# Defaults
# --------

# Functions
# ---------

# Test if any Gnocchi services are enabled
# is_gnocchi_enabled
function is_gnocchi_enabled {
    [[ ,${ENABLED_SERVICES} =~ ,"gnocchi-" ]] && return 0
    return 1
}

# create_gnocchi_accounts() - Set up common required gnocchi accounts

# Project              User            Roles
# -------------------------------------------------------------------------
# $SERVICE_TENANT_NAME  gnocchi        service
# gnocchi_swift         gnocchi_swift  ResellerAdmin  (if Swift is enabled)
function create_gnocchi_accounts {
    # Gnocchi
    if is_service_enabled key && is_service_enabled gnocchi-api
    then
        create_service_user "gnocchi"

        if [[ "$KEYSTONE_CATALOG_BACKEND" = 'sql' ]]; then
            local gnocchi_service=$(get_or_create_service "gnocchi" \
                "metric" "OpenStack Metric Service")
            get_or_create_endpoint $gnocchi_service \
                "$REGION_NAME" \
                "$(gnocchi_service_url)/" \
                "$(gnocchi_service_url)/" \
                "$(gnocchi_service_url)/"
        fi
        if is_service_enabled swift && [[ "$GNOCCHI_STORAGE_BACKEND" = 'swift' ]] ; then
            get_or_create_project "gnocchi_swift" default
            local gnocchi_swift_user=$(get_or_create_user "gnocchi_swift" \
                "$SERVICE_PASSWORD" default "gnocchi_swift@example.com")
            get_or_add_user_project_role "ResellerAdmin" $gnocchi_swift_user "gnocchi_swift"
        fi
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
        restart_service redis
    fi

    pip_install_gr redis
}

# install influxdb
# NOTE(chdent): InfluxDB is not currently packaged by the distro at the
# version that gnocchi needs. Until that is true we're downloading
# the debs and rpms packaged by the InfluxDB company. When it is
# true this method can be changed to be similar to
# _gnocchi_install_redis above.
function _gnocchi_install_influxdb {
    if is_package_installed influxdb; then
        echo "influxdb already installed"
    else
        local file=$(mktemp /tmp/influxpkg-XXXXX)

        if is_ubuntu; then
            wget -O $file $GNOCCHI_INFLUXDB_DEB_PKG
            sudo dpkg -i $file
        elif is_fedora; then
            wget -O $file $GNOCCHI_INFLUXDB_RPM_PKG
            sudo rpm -i $file
        fi
        rm $file
    fi

    # restart influxdb via its initscript
    sudo /opt/influxdb/init.sh restart
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

    git_clone ${GRAFANA_PLUGINS_REPO} ${GRAFANA_PLUGINS_DIR}
    # Grafana-server does not handle symlink :(
    sudo mkdir -p /usr/share/grafana/public/app/plugins/datasource/gnocchi
    sudo mount -o bind ${GRAFANA_PLUGINS_DIR}/datasources/gnocchi /usr/share/grafana/public/app/plugins/datasource/gnocchi

    sudo service grafana-server restart
}

# remove the influxdb database
function _gnocchi_cleanup_influxdb {
    curl -G 'http://localhost:8086/query' --data-urlencode "q=DROP DATABASE $GNOCCHI_INFLUXDB_DBNAME"
}

function _cleanup_gnocchi_apache_wsgi {
    sudo rm -f $GNOCCHI_WSGI_DIR/*.wsgi
    sudo rm -f $(apache_site_config_for gnocchi)
}

# _config_gnocchi_apache_wsgi() - Set WSGI config files of Gnocchi
function _config_gnocchi_apache_wsgi {
    sudo mkdir -p $GNOCCHI_WSGI_DIR

    local gnocchi_apache_conf=$(apache_site_config_for gnocchi)
    local venv_path=""
    local script_name=$GNOCCHI_SERVICE_PREFIX

    if [[ ${USE_VENV} = True ]]; then
        venv_path="python-path=${PROJECT_VENV["gnocchi"]}/lib/$(python_version)/site-packages"
    fi

    # copy wsgi file
    sudo cp $GNOCCHI_DIR/gnocchi/rest/app.wsgi $GNOCCHI_WSGI_DIR/

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
            s|%WSGI%|$GNOCCHI_WSGI_DIR/app.wsgi|g;
            s|%USER%|$STACK_USER|g
            s|%APIWORKERS%|$API_WORKERS|g
            s|%VIRTUALENV%|$venv_path|g
        " -i $gnocchi_apache_conf
}



# cleanup_gnocchi() - Remove residual data files, anything left over from previous
# runs that a clean run would need to clean up
function cleanup_gnocchi {
    if [ "$GNOCCHI_USE_MOD_WSGI" == "True" ]; then
        _cleanup_gnocchi_apache_wsgi
    fi
}

# configure_gnocchi() - Set config files, create data dirs, etc
function configure_gnocchi {
    [ ! -d $GNOCCHI_DATA_DIR ] && sudo mkdir -m 755 -p $GNOCCHI_DATA_DIR
    sudo chown $STACK_USER $GNOCCHI_DATA_DIR

    # Configure logging
    iniset $GNOCCHI_CONF DEFAULT verbose True
    iniset $GNOCCHI_CONF DEFAULT debug "$ENABLE_DEBUG_LOG_LEVEL"

    # Install the policy file for the API server
    cp $GNOCCHI_DIR/etc/gnocchi/policy.json $GNOCCHI_CONF_DIR

    iniset $GNOCCHI_CONF storage coordination_url "$GNOCCHI_COORDINATOR_URL"
    if [ "${GNOCCHI_COORDINATOR_URL:0:7}" == "file://" ]; then
        gnocchi_locks_dir=${GNOCCHI_COORDINATOR_URL:7}
        [ ! -d $gnocchi_locks_dir ] && sudo mkdir -m 755 -p ${gnocchi_locks_dir}
        sudo chown $STACK_USER $gnocchi_locks_dir
    fi

    # Configure auth token middleware
    configure_auth_token_middleware $GNOCCHI_CONF gnocchi $GNOCCHI_AUTH_CACHE_DIR

    # Configure the storage driver
    if is_service_enabled ceph && [[ "$GNOCCHI_STORAGE_BACKEND" = 'ceph' ]] ; then
        iniset $GNOCCHI_CONF storage driver ceph
        iniset $GNOCCHI_CONF storage ceph_username ${GNOCCHI_CEPH_USER}
        iniset $GNOCCHI_CONF storage ceph_keyring ${CEPH_CONF_DIR}/ceph.client.${GNOCCHI_CEPH_USER}.keyring
    elif is_service_enabled swift && [[ "$GNOCCHI_STORAGE_BACKEND" = 'swift' ]] ; then
        iniset $GNOCCHI_CONF storage driver swift
        iniset $GNOCCHI_CONF storage swift_user gnocchi_swift
        iniset $GNOCCHI_CONF storage swift_key $SERVICE_PASSWORD
        iniset $GNOCCHI_CONF storage swift_tenant_name "gnocchi_swift"
        iniset $GNOCCHI_CONF storage swift_auth_version 2
        iniset $GNOCCHI_CONF storage swift_authurl $KEYSTONE_SERVICE_URI/v2.0/
    elif [[ "$GNOCCHI_STORAGE_BACKEND" = 'file' ]] ; then
        iniset $GNOCCHI_CONF storage driver file
        iniset $GNOCCHI_CONF storage file_basepath $GNOCCHI_DATA_DIR/
    elif [[ "$GNOCCHI_STORAGE_BACKEND" == 'influxdb' ]] ; then
        iniset $GNOCCHI_CONF storage driver influxdb
        iniset $GNOCCHI_CONF storage influxdb_database $GNOCCHI_INFLUXDB_DBNAME
    else
        echo "ERROR: could not configure storage driver"
        exit 1
    fi

    if is_service_enabled key; then
        if is_service_enabled gnocchi-grafana; then
            iniset_multiline $GNOCCHI_CONF api middlewares oslo_middleware.cors.CORS keystonemiddleware.auth_token.AuthProtocol
            iniset $KEYSTONE_CONF cors allowed_origin ${GRAFANA_URL}
            iniset $GNOCCHI_CONF cors allowed_origin ${GRAFANA_URL}
            iniset $GNOCCHI_CONF cors allow_methods GET,POST,PUT,DELETE,OPTIONS,HEAD,PATCH
            iniset $GNOCCHI_CONF cors allow_headers Content-Type,Cache-Control,Content-Language,Expires,Last-Modified,Pragma,X-Auth-Token,X-Subject-Token
        else
            iniset $GNOCCHI_CONF api middlewares keystonemiddleware.auth_token.AuthProtocol
        fi
    else
        iniset $GNOCCHI_CONF api middlewares ""
    fi

    # Configure the indexer database
    iniset $GNOCCHI_CONF indexer url `database_connection_url gnocchi`

    if [ "$GNOCCHI_USE_MOD_WSGI" == "True" ]; then
        _config_gnocchi_apache_wsgi
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
    sudo ceph -c ${CEPH_CONF_FILE} auth get-or-create client.${GNOCCHI_CEPH_USER} mon "allow r" osd "allow class-read object_prefix rbd_children, allow rwx pool=${GNOCCHI_CEPH_POOL}" | sudo tee ${CEPH_CONF_DIR}/ceph.client.${GNOCCHI_CEPH_USER}.keyring
    sudo chown ${STACK_USER}:$(id -g -n $whoami) ${CEPH_CONF_DIR}/ceph.client.${GNOCCHI_CEPH_USER}.keyring
}

function configure_ceilometer_gnocchi {
    gnocchi_url=$(gnocchi_service_url)
    iniset $CEILOMETER_CONF DEFAULT dispatcher gnocchi
    iniset $CEILOMETER_CONF alarms gnocchi_url $gnocchi_url
    iniset $CEILOMETER_CONF dispatcher_gnocchi url $gnocchi_url
    iniset $CEILOMETER_CONF dispatcher_gnocchi archive_policy ${GNOCCHI_ARCHIVE_POLICY}
    if is_service_enabled swift && [[ "$GNOCCHI_STORAGE_BACKEND" = 'swift' ]] ; then
        iniset $CEILOMETER_CONF dispatcher_gnocchi filter_service_activity "True"
        iniset $CEILOMETER_CONF dispatcher_gnocchi filter_project "gnocchi_swift"
    else
        iniset $CEILOMETER_CONF dispatcher_gnocchi filter_service_activity "False"
    fi
}

function configure_aodh_gnocchi {
    gnocchi_url=$(gnocchi_service_url)
    iniset $AODH_CONF DEFAULT gnocchi_url $gnocchi_url
}


# init_gnocchi() - Initialize etc.
function init_gnocchi {
    # Create cache dir
    sudo mkdir -p $GNOCCHI_AUTH_CACHE_DIR
    sudo chown $STACK_USER $GNOCCHI_AUTH_CACHE_DIR
    rm -f $GNOCCHI_AUTH_CACHE_DIR/*

    if is_service_enabled mysql postgresql; then
        recreate_database gnocchi utf8
        $GNOCCHI_BIN_DIR/gnocchi-dbsync
    fi
}

# install_gnocchi() - Collect source and prepare
function install_gnocchi {
    if [ "${GNOCCHI_COORDINATOR_URL%%:*}" == "redis" ]; then
        _gnocchi_install_redis
    fi

    if [[ "${GNOCCHI_STORAGE_BACKEND}" == 'influxdb' ]] ; then
        _gnocchi_install_influxdb
        pip_install influxdb
    fi

    if is_service_enabled gnocchi-grafana
    then
        _gnocchi_install_grafana
    fi

    # NOTE(sileht): requirements are not merged with the global-requirement repo
    # setup_develop $GNOCCHI_DIR
    USE_CONSTRAINTS=False setup_package $GNOCCHI_DIR -e

    if [ "$GNOCCHI_USE_MOD_WSGI" == "True" ]; then
        install_apache_wsgi
    fi

    # Create configuration directory
    [ ! -d $GNOCCHI_CONF_DIR ] && sudo mkdir -m 755 -p $GNOCCHI_CONF_DIR
    sudo chown $STACK_USER $GNOCCHI_CONF_DIR
}

# start_gnocchi() - Start running processes, including screen
function start_gnocchi {
    local token

    run_process gnocchi-metricd "$GNOCCHI_BIN_DIR/gnocchi-metricd -d -v --config-file $GNOCCHI_CONF"

    if [ "$GNOCCHI_USE_MOD_WSGI" == "True" ]; then
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
            tail_log gnocchi /var/log/$APACHE_NAME/error[._]log
            tail_log gnocchi-api /var/log/$APACHE_NAME/access[._]log
        fi
    else
        run_process gnocchi-api "$GNOCCHI_BIN_DIR/gnocchi-api -d -v --config-file $GNOCCHI_CONF"
    fi
    # only die on API if it was actually intended to be turned on
    if is_service_enabled gnocchi-api; then
        echo "Waiting for gnocchi-api to start..."
        if ! timeout $SERVICE_TIMEOUT sh -c "while ! curl --noproxy '*' -s $(gnocchi_service_url)/v1/resource/generic >/dev/null; do sleep 1; done"; then
            die $LINENO "gnocchi-api did not start"
        fi
    fi

    # Create a default policy
    archive_policy_url="$(gnocchi_service_url)/v1/archive_policy"
    if is_service_enabled key; then
        token=$(openstack token issue -f value -c id)
        create_archive_policy() { curl -X POST -H "X-Auth-Token: $token" -H "Content-Type: application/json" -d "$1" $archive_policy_url ; }
    else
        userid=`uuidgen`
        projectid=`uuidgen`
        create_archive_policy() { curl -X POST -H "X-ROLES: admin" -H "X-USER-ID: $userid" -H "X-PROJECT-ID: $projectid" -H "Content-Type: application/json" -d "$1" $archive_policy_url ; }
    fi

    create_archive_policy '{"name":"low","definition":[{"granularity": "5m","points": 12},{"granularity": "1h","points": 24},{"granularity": "1d","points": 30}]}'
    create_archive_policy '{"name":"medium","definition":[{"granularity": "60s","points": 60},{"granularity": "1h","points": 168},{"granularity": "1d","points": 365}]}'
    create_archive_policy '{"name":"high","definition":[{"granularity": "1s","points": 86400},{"granularity": "1m","points": 43200},{"granularity": "1h","points": 8760}]}'
}

# stop_gnocchi() - Stop running processes
function stop_gnocchi {
    if [ "$GNOCCHI_USE_MOD_WSGI" == "True" ]; then
        disable_apache_site gnocchi
        restart_apache_server
    fi
    # Kill the gnocchi screen windows
    for serv in gnocchi-api; do
        stop_process $serv
    done

    if [[ "${GNOCCHI_STORAGE_BACKEND}" == 'influxdb' ]] ; then
        _gnocchi_cleanup_influxdb
    fi

    if is_service_enabled gnocchi-grafana; then
        sudo umount /usr/share/grafana/public/app/plugins/datasource/gnocchi
    fi
}

if is_service_enabled gnocchi-api; then
    if [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo_summary "Installing Gnocchi"
        stack_install_service gnocchi
    elif [[ "$1" == "stack" && "$2" == "post-config" ]]; then
        echo_summary "Configuring Gnocchi"
        configure_gnocchi
        create_gnocchi_accounts
        if is_service_enabled ceilometer; then
            echo_summary "Configuring Ceilometer for gnocchi"
            configure_ceilometer_gnocchi
        fi
        if is_service_enabled aodh; then
            echo_summary "Configuring Aodh for gnocchi"
            configure_aodh_gnocchi
        fi
        if is_service_enabled ceph && [[ "$GNOCCHI_STORAGE_BACKEND" = 'ceph' ]] ; then
            echo_summary "Configuring Gnocchi for Ceph"
            configure_ceph_gnocchi
        fi
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
