package mysqllock

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/magic-lib/go-plat-locker/internal/config"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	defaultTableName         = "mysql_locker_records"
	clearExpirationHour uint = 1
	// 错误定义
	ErrLockTimeout  = errors.New("获取锁超时")
	ErrNotRunLock   = errors.New("没有执行Lock方法，直接执行了UnLock")
	ErrNotLockOwner = errors.New("不是锁的持有者")
)

// DBLock 数据库锁接口
type DBLock interface {
	TryLock(ctx context.Context) (bool, error) // 尝试获取锁（非阻塞）
	Lock(ctx context.Context) error            // 获取锁（阻塞）
	UnLock(ctx context.Context) error          // 释放锁
	IsHeld(ctx context.Context) (bool, error)  // 检查锁是否被当前持有者持有
	Renew(ctx context.Context) error           // 续期锁
}

type Config struct {
	DSN       string `json:"dsn"`
	SqlDB     *sql.DB
	TableName string `json:"table_name"`
	Namespace string `json:"namespace"`
}

// MySqlLock MySQL 分布式锁实现
type MySqlLock struct {
	sqlDB      *sql.DB       // 数据库连接
	namespace  string        // 命名空间
	tableName  string        // 锁表名
	lockKey    string        // 锁键
	lockValue  string        // 锁持有者标识
	expiration time.Duration // 锁过期时间

	mu        sync.Mutex // 本地锁，用于可重入
	reentrant int        // 可重入计数
}

// NewMySqlLock 创建一个新的 MySQL 锁
func NewMySqlLock(cfg *Config, key string, expiration time.Duration) (*MySqlLock, error) {
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("namespace is empty")
	}

	if cfg.TableName == "" {
		cfg.TableName = defaultTableName
	}
	if cfg.SqlDB == nil {
		if cfg.DSN != "" {
			sqlDB, err := sql.Open("mysql", cfg.DSN)
			if err != nil {
				return nil, fmt.Errorf("初始化数据库连接失败: %v", err)
			}
			cfg.SqlDB = sqlDB
		}
	}
	if cfg.SqlDB == nil {
		return nil, fmt.Errorf("sqldb is empty")
	}

	rs := new(MySqlLock)
	rs.sqlDB = cfg.SqlDB
	rs.tableName = cfg.TableName
	rs.namespace = cfg.Namespace

	err := rs.createLockTable()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 16)
	_, err = rand.Read(b)
	if err != nil {
		return nil, err
	}
	v := base64.StdEncoding.EncodeToString(b)

	rs.sqlDB = cfg.SqlDB
	rs.lockKey = key
	rs.lockValue = v
	rs.expiration = expiration

	err = rs.clearAllExpireRecords(clearExpirationHour)
	if err != nil {
		log.Println("Error clearing expired records:", err)
	}

	return rs, nil
}

func (l *MySqlLock) createLockTable() error {
	creatSql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
	  id bigint NOT NULL AUTO_INCREMENT,
	  namespace varchar(64) NOT NULL COMMENT '命名空间',
	  lock_key varchar(64) NOT NULL COMMENT '锁键',
	  lock_value varchar(64) NOT NULL COMMENT '锁持有者标识',
	  expire_time datetime NOT NULL COMMENT '过期时间',
	  create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	  PRIMARY KEY (id),
	  UNIQUE KEY uk_lock_key (namespace, lock_key)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分布式锁表';
    `, l.tableName)
	_, err := l.sqlDB.Exec(creatSql)
	if err != nil {
		return err
	}
	return nil
}

func (l *MySqlLock) checkToGetLock(ctx context.Context) (bool, error) {
	// 检查是否已持有锁
	if l.reentrant > 0 {
		held, err := l.isHeldUnlocked(ctx)
		if err != nil {
			return false, err
		}
		if held {
			l.reentrant++
		}
		return false, nil
	}

	return true, nil
}
func (l *MySqlLock) tryToLocked(ctx context.Context) (bool, error) {
	acquired, err := l.tryLockUnlocked(ctx)
	if err != nil {
		return false, err
	}
	if acquired {
		l.reentrant = 1
		return true, nil
	}
	return false, nil
}

// Lock 获取锁（阻塞）
func (l *MySqlLock) Lock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	canLock, err := l.checkToGetLock(ctx)
	if err != nil {
		return err
	}
	if !canLock {
		return nil
	}

	// 尝试获取锁
	timeout := time.NewTimer(l.expiration)
	defer timeout.Stop()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		acquired, err := l.tryToLocked(ctx)
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return ErrLockTimeout
		case <-ticker.C:
			// 继续尝试
		}
	}
}

// TryLock 尝试获取锁（非阻塞）
func (l *MySqlLock) TryLock(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	canLock, err := l.checkToGetLock(ctx)
	if err != nil {
		return false, err
	}
	if !canLock {
		return false, nil
	}

	return l.tryToLocked(ctx)
}

// UnLock 释放锁
func (l *MySqlLock) UnLock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 没有加锁，直接执行UnLock逻辑，直接报错
	if l.reentrant <= 0 {
		return ErrNotRunLock
	}

	// 解锁一个锁
	l.reentrant--
	if l.reentrant > 0 {
		//表示还没有全部解锁完
		return nil
	}

	sqlStr := fmt.Sprintf(`DELETE FROM %s WHERE namespace=? AND lock_key = ? AND lock_value = ?`, l.tableName)
	ctx = context.Background()
	// 执行释放锁操作
	result, err := l.sqlDB.ExecContext(ctx, sqlStr, l.namespace, l.lockKey, l.lockValue)
	if err != nil {
		return err
	}

	err = l.clearAllExpireRecords(clearExpirationHour)
	if err != nil {
		log.Println("Error clearing expired records:", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return ErrNotLockOwner
	}

	return nil
}

// IsHeld 检查锁是否被当前持有者持有
func (l *MySqlLock) IsHeld(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.isHeldUnlocked(ctx)
}

// Renew 继续锁
func (l *MySqlLock) Renew(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 检查是否持有锁
	if l.reentrant <= 0 {
		return ErrNotRunLock
	}

	// 执行续期操作
	expireAt := time.Now().Add(l.expiration).Format("2006-01-02 15:04:05")
	result, err := l.sqlDB.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET expire_time = ? WHERE namespace=? AND lock_key = ? AND lock_value = ?`, l.tableName), expireAt, l.namespace, l.lockKey, l.lockValue)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return ErrNotLockOwner
	}

	return nil
}

// tryLockUnlocked 尝试获取锁（不包含本地锁）
func (l *MySqlLock) tryLockUnlocked(ctx context.Context) (bool, error) {
	expireTime := time.Now().Add(l.expiration).Format("2006-01-02 15:04:05")
	createTime := time.Now().Format("2006-01-02 15:04:05")

	// 使用INSERT ... ON DUPLICATE KEY UPDATE实现锁
	returned, err := l.sqlDB.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (namespace, lock_key, lock_value, expire_time, create_time)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE 
			lock_value = IF(expire_time < '%s', VALUES(lock_value), lock_value),
			expire_time = IF(expire_time < '%s', VALUES(expire_time), expire_time)
	`, l.tableName, createTime, createTime), l.namespace, l.lockKey, l.lockValue, expireTime, createTime)

	if err != nil {
		// 可能是唯一键冲突，但锁未过期
		return false, err
	}

	affectNum, err := returned.RowsAffected()
	if err != nil {
		return false, err
	}
	if affectNum == 0 {
		return false, nil
	}

	return true, nil
}

// isHeldUnlocked 检查锁是否被当前持有者持有（不包含本地锁）
func (l *MySqlLock) isHeldUnlocked(ctx context.Context) (bool, error) {
	nowTime := time.Now().Format("2006-01-02 15:04:05")
	var count int
	err := l.sqlDB.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM %s 
		WHERE namespace=? AND lock_key = ? AND lock_value = ? AND expire_time > '%s'
	`, l.tableName, nowTime), l.namespace, l.lockKey, l.lockValue).Scan(&count)

	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (l *MySqlLock) clearAllExpireRecords(hours uint) error {
	if hours == 0 {
		return nil
	}
	nowTime := time.Now().Format("2006-01-02 15:04:05")
	clearTime := time.Now().Add(-time.Duration(hours) * time.Hour).Format("2006-01-02 15:04:05")

	sqlStr := fmt.Sprintf(`DELETE FROM %s WHERE expire_time < '%s' AND expire_time < '%s'`, l.tableName, nowTime, clearTime)
	_, err := l.sqlDB.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

func (l *MySqlLock) TryLockFunc(ctx context.Context, f func()) (bool, error) {
	ok, err := l.TryLock(ctx)
	if err != nil {
		return false, err
	}
	if ok {
		defer func() {
			_ = l.UnLock(ctx)
		}()
		f()
		return true, nil
	}
	return false, nil
}

func (l *MySqlLock) LockFunc(ctx context.Context, f func()) error {
	err := l.Lock(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = l.UnLock(ctx)
	}()
	f()
	return nil
}

func (l *MySqlLock) LockerType() string {
	return config.LockerTypeMysql
}
