# Gnocchi devstack plugin
# Install and start **Gnocchi** service

# To enable Gnocchi service, add the following to localrc:
#
#   enable_plugin gnocchi https://github.com/openstack/gnocchi master
#   enable_service gnocchi-api
#

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
set +o xtrace


# Defaults
# --------

# Setup repository
GNOCCHI_REPO=${GNOCCHI_REPO:-${GIT_BASE}/openstack/gnocchi.git}
GNOCCHI_BRANCH=${GNOCCHI_BRANCH:-master}

# Set up default directories
GNOCCHI_DIR=$DEST/gnocchi
GNOCCHI_CONF_DIR=/etc/gnocchi
GNOCCHI_CONF=$GNOCCHI_CONF_DIR/gnocchi.conf
GNOCCHI_API_LOG_DIR=/var/log/gnocchi-api
GNOCCHI_AUTH_CACHE_DIR=${GNOCCHI_AUTH_CACHE_DIR:-/var/cache/gnocchi}
GNOCCHI_WSGI_DIR=${GNOCCHI_WSGI_DIR:-/var/www/gnocchi}
GNOCCHI_DATA_DIR=${GNOCCHI_DATA_DIR:-${DATA_DIR}/gnocchi}
GNOCCHI_COORDINATOR_URL=${GNOCCHI_COORDINATOR_URL:-file://${GNOCCHI_DATA_DIR}/locks}

# Toggle for deploying Gnocchi under HTTPD + mod_wsgi
GNOCCHI_USE_MOD_WSGI=${GNOCCHI_USE_MOD_WSGI:-${ENABLE_HTTPD_MOD_WSGI_SERVICES}}

# Support potential entry-points console scripts
GNOCCHI_BIN_DIR=$(get_python_exec_prefix)

# Gnocchi connection info.
GNOCCHI_SERVICE_PROTOCOL=http
GNOCCHI_SERVICE_HOST=$SERVICE_HOST
GNOCCHI_SERVICE_PORT=${GNOCCHI_SERVICE_PORT:-8041}

# Gnocchi ceilometer default archive_policy
GNOCCHI_ARCHIVE_POLICY=${GNOCCHI_ARCHIVE_POLICY:-low}

# ceph gnochi info
GNOCCHI_CEPH_USER=${GNOCCHI_CEPH_USER:-gnocchi}
GNOCCHI_CEPH_POOL=${GNOCCHI_CEPH_POOL:-gnocchi}
GNOCCHI_CEPH_POOL_PG=${GNOCCHI_CEPH_POOL_PG:-8}
GNOCCHI_CEPH_POOL_PGP=${GNOCCHI_CEPH_POOL_PGP:-8}

# Gnocchi with keystone
GNOCCHI_USE_KEYSTONE=${GNOCCHI_USE_KEYSTONE:-True}

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
create_gnocchi_accounts() {
    # Gnocchi
    if [[ "$ENABLED_SERVICES" =~ "gnocchi-api" ]]; then
        create_service_user "gnocchi"

        if [[ "$KEYSTONE_CATALOG_BACKEND" = 'sql' ]]; then
            local gnocchi_service=$(get_or_create_service "gnocchi" \
                "metric" "OpenStack Metric Service")
            get_or_create_endpoint $gnocchi_service \
                "$REGION_NAME" \
                "$GNOCCHI_SERVICE_PROTOCOL://$GNOCCHI_SERVICE_HOST:$GNOCCHI_SERVICE_PORT/" \
                "$GNOCCHI_SERVICE_PROTOCOL://$GNOCCHI_SERVICE_HOST:$GNOCCHI_SERVICE_PORT/" \
                "$GNOCCHI_SERVICE_PROTOCOL://$GNOCCHI_SERVICE_HOST:$GNOCCHI_SERVICE_PORT/"
        fi
        if is_service_enabled swift; then
            get_or_create_project "gnocchi_swift"
            local gnocchi_swift_user=$(get_or_create_user "gnocchi_swift" \
                "$SERVICE_PASSWORD" "gnocchi_swift@example.com")
            get_or_add_user_project_role "ResellerAdmin" $gnocchi_swift_user "gnocchi_swift"
        fi
    fi
}

function _cleanup_gnocchi_apache_wsgi {
    sudo rm -f $GNOCCHI_WSGI_DIR/*.wsgi
    sudo rm -f $(apache_site_config_for gnocchi)
}

# _config_gnocchi_apache_wsgi() - Set WSGI config files of Keystone
function _config_gnocchi_apache_wsgi {
    sudo mkdir -p $GNOCCHI_WSGI_DIR

    local gnocchi_apache_conf=$(apache_site_config_for gnocchi)

    # copy proxy vhost and wsgi file
    sudo cp $GNOCCHI_DIR/gnocchi/rest/app.wsgi $GNOCCHI_WSGI_DIR/

    sudo cp $GNOCCHI_DIR/devstack/apache-gnocchi.template $gnocchi_apache_conf
    sudo sed -e "
        s|%GNOCCHI_PORT%|$GNOCCHI_SERVICE_PORT|g;
        s|%APACHE_NAME%|$APACHE_NAME|g;
        s|%WSGI%|$GNOCCHI_WSGI_DIR/app.wsgi|g;
        s|%USER%|$STACK_USER|g
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
    [ ! -d $GNOCCHI_CONF_DIR ] && sudo mkdir -m 755 -p $GNOCCHI_CONF_DIR
    sudo chown $STACK_USER $GNOCCHI_CONF_DIR

    [ ! -d $GNOCCHI_API_LOG_DIR ] &&  sudo mkdir -m 755 -p $GNOCCHI_API_LOG_DIR
    sudo chown $STACK_USER $GNOCCHI_API_LOG_DIR

    [ ! -d $GNOCCHI_DATA_DIR ] && sudo mkdir -m 755 -p $GNOCCHI_DATA_DIR
    sudo chown $STACK_USER $GNOCCHI_DATA_DIR

    # Configure logging
    iniset $GNOCCHI_CONF DEFAULT verbose True
    if [ "$GNOCCHI_USE_MOD_WSGI" != "True" ]; then
        iniset $GNOCCHI_CONF DEFAULT debug "$ENABLE_DEBUG_LOG_LEVEL"
    fi

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
    if is_service_enabled ceph; then
        iniset $GNOCCHI_CONF storage driver ceph
        iniset $GNOCCHI_CONF storage ceph_username ${GNOCCHI_CEPH_USER}
        iniset $GNOCCHI_CONF storage ceph_keyring ${CEPH_CONF_DIR}/ceph.client.${GNOCCHI_CEPH_USER}.keyring
    elif is_service_enabled swift; then
        iniset $GNOCCHI_CONF storage driver swift
        iniset $GNOCCHI_CONF storage swift_user gnocchi_swift
        iniset $GNOCCHI_CONF storage swift_key $SERVICE_PASSWORD
        iniset $GNOCCHI_CONF storage swift_tenant_name "gnocchi_swift"
        iniset $GNOCCHI_CONF storage swift_auth_version 2
        iniset $GNOCCHI_CONF storage swift_authurl $KEYSTONE_SERVICE_URI/v2.0/
    else
        iniset $GNOCCHI_CONF storage driver file
        iniset $GNOCCHI_CONF storage file_basepath $GNOCCHI_DATA_DIR/
    fi

    if [ "$GNOCCHI_STORAGE_BACKEND" ]; then
        iniset $GNOCCHI_CONF storage driver "$GNOCCHI_STORAGE_BACKEND"
    fi

    if [ "$GNOCCHI_USE_KEYSTONE" != "True" ]; then
        iniset $GNOCCHI_CONF api middlewares ""
    else
        inicomment $GNOCCHI_CONF api middlewares
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
    gnocchi_url=$GNOCCHI_SERVICE_PROTOCOL://$GNOCCHI_SERVICE_HOST:$GNOCCHI_SERVICE_PORT
    iniset $CEILOMETER_CONF DEFAULT dispatcher gnocchi
    iniset $CEILOMETER_CONF alarms gnocchi_url $gnocchi_url
    iniset $CEILOMETER_CONF dispatcher_gnocchi url $gnocchi_url
    iniset $CEILOMETER_CONF dispatcher_gnocchi archive_policy ${GNOCCHI_ARCHIVE_POLICY}
    if is_service_enabled swift; then
        iniset $CEILOMETER_CONF dispatcher_gnocchi filter_service_activity "True"
        iniset $CEILOMETER_CONF dispatcher_gnocchi filter_project "gnocchi_swift"
    else
        iniset $CEILOMETER_CONF dispatcher_gnocchi filter_service_activity "False"
    fi
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
    git_clone $GNOCCHI_REPO $GNOCCHI_DIR $GNOCCHI_BRANCH

    # NOTE(sileht): requirements are not yet merged with the global-requirement repo
    # setup_develop $GNOCCHI_DIR
    setup_package $GNOCCHI_DIR -e

    if [ "$GNOCCHI_USE_MOD_WSGI" == "True" ]; then
        install_apache_wsgi
    fi

}

# start_gnocchi() - Start running processes, including screen
function start_gnocchi {
    local token

    if [ "$GNOCCHI_USE_MOD_WSGI" == "True" ]; then
        enable_apache_site gnocchi
        restart_apache_server
        tail_log gnocchi /var/log/$APACHE_NAME/gnocchi.log
        tail_log gnocchi-api /var/log/$APACHE_NAME/gnocchi-access.log
    else
        run_process gnocchi-api "gnocchi-api -d -v --log-dir=$GNOCCHI_API_LOG_DIR --config-file $GNOCCHI_CONF"
    fi
    # only die on API if it was actually intended to be turned on
    if is_service_enabled gnocchi-api; then
        echo "Waiting for gnocchi-api to start..."
        if ! timeout $SERVICE_TIMEOUT sh -c "while ! curl --noproxy '*' -s ${GNOCCHI_SERVICE_PROTOCOL}://${GNOCCHI_SERVICE_HOST}:${GNOCCHI_SERVICE_PORT}/v1/resource/generic >/dev/null; do sleep 1; done"; then
            die $LINENO "gnocchi-api did not start"
        fi
    fi

    # Create a default policy
    archive_policy_url="${GNOCCHI_SERVICE_PROTOCOL}://${GNOCCHI_SERVICE_HOST}:${GNOCCHI_SERVICE_PORT}/v1/archive_policy"
    if [ "$GNOCCHI_USE_KEYSTONE" == "True" ]; then
        token=$(openstack token issue -f value -c id)
        create_archive_policy() { curl -X POST -H "X-Auth-Token: $token" -H "Content-Type: application/json" -d "$1" $archive_policy_url ; }
    else
        userid=$(openstack user show admin -f value -c id)
        projectid=$(openstack project show admin -f value -c id)
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
}

if is_service_enabled gnocchi-api; then
    if [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo_summary "Installing Gnocchi"
        install_gnocchi
    elif [[ "$1" == "stack" && "$2" == "post-config" ]]; then
        echo_summary "Configuring Gnocchi"
        configure_gnocchi
        create_gnocchi_accounts
    elif [[ "$1" == "stack" && "$2" == "extra" ]]; then
        echo_summary "Initializing Gnocchi"
        if is_service_enabled ceilometer; then
            echo_summary "Configuring Ceilometer for gnocchi"
            configure_ceilometer_gnocchi
        fi
        if is_service_enabled ceph; then
            echo_summary "Configuring Gnocchi for Ceph"
            configure_ceph_gnocchi
        fi
        init_gnocchi
        start_gnocchi
    fi

    if [[ "$1" == "unstack" ]]; then
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


