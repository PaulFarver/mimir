package memory

import (
	"database/sql"

	"github.com/weaveworks/cortex/configs"
)

type config struct {
	cfg configs.Config
	id  configs.ID
}

func (c config) toView() configs.ConfigView {
	return configs.ConfigView{
		ID:     c.id,
		Config: c.cfg,
	}
}

// DB is an in-memory database for testing, and local development
type DB struct {
	cfgs map[configs.OrgID]config
	id   uint
}

// New creates a new in-memory database
func New(_, _ string) (*DB, error) {
	return &DB{
		cfgs: map[configs.OrgID]config{},
		id:   0,
	}, nil
}

// GetOrgConfig gets the org's configuration.
func (d *DB) GetOrgConfig(orgID configs.OrgID) (configs.ConfigView, error) {
	c, ok := d.cfgs[orgID]
	if !ok {
		return configs.ConfigView{}, sql.ErrNoRows
	}
	return c.toView(), nil
}

// SetOrgConfig sets configuration for a org.
func (d *DB) SetOrgConfig(orgID configs.OrgID, cfg configs.Config) error {
	d.cfgs[orgID] = config{cfg: cfg, id: configs.ID(d.id)}
	d.id++
	return nil
}

// GetAllOrgConfigs gets all of the organization configs for a subsystem.
func (d *DB) GetAllOrgConfigs() (map[configs.OrgID]configs.ConfigView, error) {
	cfgs := map[configs.OrgID]configs.ConfigView{}
	for org, c := range d.cfgs {
		cfgs[org] = c.toView()
	}
	return cfgs, nil
}

// GetOrgConfigs gets all of the organization configs for a subsystem that
// have changed recently.
func (d *DB) GetOrgConfigs(since configs.ID) (map[configs.OrgID]configs.ConfigView, error) {
	cfgs := map[configs.OrgID]configs.ConfigView{}
	for org, c := range d.cfgs {
		if c.id > since {
			cfgs[org] = c.toView()
		}
	}
	return cfgs, nil
}

// Close finishes using the db. Noop.
func (d *DB) Close() error {
	return nil
}
