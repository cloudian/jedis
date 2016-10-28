package redis.clients.jedis;

import java.net.URI;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.JedisURIHelper;
import redis.clients.util.Pool;

public class JedisPool extends Pool<Jedis> implements JedisPoolMBean {

    protected static Logger logger = LoggerFactory.getLogger(JedisPool.class);
    private JedisFactory factory;

  public JedisPool() {
    this(Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host) {
    this(poolConfig, host, Protocol.DEFAULT_PORT, Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(String host, int port) {
    this(new GenericObjectPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE, null);
  }

    public JedisPool(final String host, final int port, final String poolName) {
        this(host, port);
        register(poolName);
    }


  public JedisPool(final String host) {
    URI uri = URI.create(host);
    if (JedisURIHelper.isValid(uri)) {
      String h = uri.getHost();
      int port = uri.getPort();
      String password = JedisURIHelper.getPassword(uri);
      int database = JedisURIHelper.getDBIndex(uri);
      this.internalPool = new GenericObjectPool<Jedis>(new JedisFactory(h, port,
          Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, password, database, null),
          new GenericObjectPoolConfig());
    } else {
      this.internalPool = new GenericObjectPool<Jedis>(new JedisFactory(host,
          Protocol.DEFAULT_PORT, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null,
          Protocol.DEFAULT_DATABASE, null), new GenericObjectPoolConfig());
    }
  }

  public JedisPool(final URI uri) {
    this(new GenericObjectPoolConfig(), uri, Protocol.DEFAULT_TIMEOUT);
  }

  public JedisPool(final URI uri, final int timeout) {
    this(new GenericObjectPoolConfig(), uri, timeout);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password) {
    this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final String poolName, int database) {
    this(poolConfig, host, port, timeout, password, database, null);
    register(poolName);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port) {
    this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final int timeout) {
    this(poolConfig, host, port, timeout, null, Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database) {
    this(poolConfig, host, port, timeout, password, database, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database, final String clientName) {
    this(poolConfig, host, port, timeout, timeout, password, database, clientName);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName) {
    super(poolConfig, new JedisFactory(host, port, connectionTimeout, soTimeout, password,
        database, clientName));
    factory = (JedisFactory) getFactory();
    register(clientName);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri) {
    this(poolConfig, uri, Protocol.DEFAULT_TIMEOUT);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri, final int timeout) {
    this(poolConfig, uri, timeout, timeout);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout, final int soTimeout) {
    super(poolConfig, new JedisFactory(uri, connectionTimeout, soTimeout, null));
  }

  @Override
  public Jedis getResource() {
    Jedis jedis = super.getResource();
    jedis.setDataSource(this);
    return jedis;
  }

  /**
   * @deprecated starting from Jedis 3.0 this method will not be exposed. Resource cleanup should be
   *             done using @see {@link redis.clients.jedis.Jedis#close()}
   */
  @Override
  @Deprecated
  public void returnBrokenResource(final Jedis resource) {
    if (resource != null) {
      returnBrokenResourceObject(resource);
    }
  }

  /**
   * @deprecated starting from Jedis 3.0 this method will not be exposed. Resource cleanup should be
   *             done using @see {@link redis.clients.jedis.Jedis#close()}
   */
  @Override
  @Deprecated
  public void returnResource(final Jedis resource) {
    if (resource != null) {
      try {
        resource.resetState();
        returnResourceObject(resource);
      } catch (Exception e) {
        returnBrokenResource(resource);
        throw new JedisException("Could not return the resource to the pool", e);
      }
    }
  }

    /**
 *      * Register itself to jmx
 *           * @param poolName
 *                */
    private void register(final String poolName) {
        final String beanName = this.getClass().getPackage().getName() + ":name=" + poolName;
        logger.info("Registering JMX " + beanName);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName on = null;
        try {
            on = new ObjectName(beanName);
        } catch (MalformedObjectNameException e) {
            logger.warn("Unable to register " + beanName, e);
            return;
        } catch (NullPointerException e) {
            logger.warn("Unable to register " + beanName, e);
            return;
        }

        if (!mbs.isRegistered(on)) {
            try {
                mbs.registerMBean(this, on);
            } catch (InstanceAlreadyExistsException e) {
                logger.warn("Unable to register " + beanName, e);
            } catch (MBeanRegistrationException e) {
                logger.warn("Unable to register " + beanName, e);
            } catch (NotCompliantMBeanException e) {
                logger.warn("Unable to register " + beanName, e);
            }
        }
    }
    @Override
    public String getHost() {
        return factory.getHost();
    }

    @Override
    public int getPort() {
        return factory.getPort();
    }

    @Override
    public int getTimeout() {
        return factory.getTimeout();
    }
    /**
     * Gracefully reset new host and port
     * 
     * @param host
     * @param port
    */
    @Override
    public void updateHostAndPort(final String host, final int port) {
        // update facotry
        factory.setHostAndPort(new HostAndPort(host, port));
        
        // Remove the idle Jedis
        clear();
        
       // the active ones will be either validated off or timed out if
       // all of the TestOn* was set to false. 
    }
        
}
