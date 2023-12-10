package bolt

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"

	dp "github.com/forscht/ddrv/internal/dataprovider"
	"github.com/forscht/ddrv/pkg/ddrv"
	"github.com/forscht/ddrv/pkg/locker"
)

const RootDirPath = "/"

type Provider struct {
	db     *bbolt.DB
	sg     *snowflake.Node
	driver *ddrv.Driver
	locker *locker.Locker
}

func New(path string, driver *ddrv.Driver) dp.DataProvider {
	db, err := bbolt.Open(path, 0666, nil)
	if err != nil {
		log.Fatal().Str("c", "bolt provider").Err(err).Msg("failed to open db")
	}
	// Initialize the filesystem root
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists([]byte("fs")); err != nil {
			return err
		}
		rootData := serializeFile(dp.File{Name: "/", Dir: true, MTime: time.Now()})
		return tx.Bucket([]byte("fs")).Put([]byte(RootDirPath), rootData)
	})
	if err != nil {
		log.Fatal().Str("c", "bolt provider").Err(err).Msg("failed to init db")
	}
	sg, err := snowflake.NewNode(int64(rand.Intn(1023)))
	if err != nil {
		log.Fatal().Err(err).Str("c", "bolt provider").Msg("failed to create snowflake node")
	}
	return &Provider{db, sg, driver, locker.New()}
}

func (bfp *Provider) Get(id, parent string) (*dp.File, error) {
	path := decodep(id)
	file, err := bfp.Stat(path)
	if err != nil {
		return nil, err
	}
	if parent != "" && string(file.Parent) != parent {
		return nil, dp.ErrNotExist
	}
	_, file.Name = filepath.Split(file.Name)
	return file, err
}

func (bfp *Provider) Update(id, parent string, file *dp.File) (*dp.File, error) {
	path := decodep(id)
	if path == RootDirPath {
		return nil, dp.ErrPermission
	}
	exciting, err := bfp.Stat(path)
	if err != nil {
		return nil, err
	}
	if parent != "" && string(exciting.Parent) != parent {
		return nil, dp.ErrInvalidParent
	}
	newp := filepath.Clean(decodep(string(file.Parent)) + "/" + file.Name)
	if err = bfp.Mv(exciting.Name, newp); err != nil {
		return nil, err
	}
	file.Name = newp
	return file, nil
}

func (bfp *Provider) GetChild(id string) ([]*dp.File, error) {
	path := decodep(id)
	file, err := bfp.Stat(path)
	if err != nil {
		return nil, err
	}
	if !file.Dir {
		return nil, dp.ErrInvalidParent
	}
	files, err := bfp.Ls(path, 0, 0)
	if err != nil {
		return nil, err
	}
	for _, file = range files {
		_, file.Name = filepath.Split(file.Name)
	}
	return files, nil
}

func (bfp *Provider) Create(name, parent string, dir bool) (*dp.File, error) {
	path := filepath.Clean(decodep(parent) + "/" + name)
	file := dp.File{Name: path, Dir: dir, MTime: time.Now()}
	err := bfp.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))
		existingFile := b.Get([]byte(path))
		if existingFile != nil {
			return dp.ErrExist
		}
		return b.Put([]byte(path), serializeFile(file))
	})
	file.Id = encodep(path)
	file.Name = name
	return &file, err
}

func (bfp *Provider) Delete(id, parent string) error {
	path := decodep(id)
	if path == RootDirPath {
		return dp.ErrPermission
	}
	file, err := bfp.Stat(path)
	if err != nil {
		return err
	}
	if parent != "" && string(file.Parent) != parent {
		return dp.ErrInvalidParent
	}
	return bfp.Rm(path)
}

func (bfp *Provider) GetNodes(id string) ([]ddrv.Node, error) {
	bfp.locker.Acquire(id)
	defer bfp.locker.Release(id)
	var nodes []ddrv.Node
	expired := make([]*ddrv.Node, 0)
	currentTimestamp := int(time.Now().Unix())
	err := bfp.db.Update(func(tx *bbolt.Tx) error {
		// Get the bucket for the specific file
		root := tx.Bucket([]byte("nodes"))
		if root == nil {
			return nil
		}
		bucket := root.Bucket([]byte(id))
		if bucket == nil {
			return nil
		}
		if err := bucket.ForEach(func(k, v []byte) error {
			var node ddrv.Node
			deserializeNode(&node, v)
			if currentTimestamp > node.Ex {
				expired = append(expired, &node)
			}
			nodes = append(nodes, node)
			return nil
		}); err != nil {
			return err
		}
		if err := bfp.driver.UpdateNodes(expired); err != nil {
			return err
		}
		for _, node := range expired {
			data := serializeNode(*node)
			key := []byte(fmt.Sprintf("%d", node.NId))
			if err := bucket.Put(key, data); err != nil {
				return err
			}
		}
		return nil
	})

	return nodes, err
}

func (bfp *Provider) CreateNodes(id string, nodes []ddrv.Node) error {
	return bfp.db.Update(func(tx *bbolt.Tx) error {
		file, err := bfp.Stat(decodep(id))
		if err != nil {
			return dp.ErrNotExist
		}
		root, err := tx.CreateBucketIfNotExists([]byte("nodes"))
		if err != nil {
			return err
		}
		bucket, err := root.CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return err
		}
		for _, node := range nodes {
			seq := bfp.sg.Generate()
			node.NId = seq.Int64()
			file.Size += int64(node.Size)
			data := serializeNode(node)
			if err := bucket.Put(seq.Bytes(), data); err != nil {
				return err
			}
		}
		data := serializeFile(*file)
		fs := tx.Bucket([]byte("fs"))
		return fs.Put([]byte(file.Name), data)
	})
}

func (bfp *Provider) DeleteNodes(id string) error {
	return bfp.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte("nodes"))
		if root == nil {
			return fmt.Errorf("root bucket missing")
		}
		return root.DeleteBucket([]byte(id))
	})
}

func (bfp *Provider) Stat(path string) (*dp.File, error) {
	path = filepath.Clean(path)
	file := new(dp.File)
	err := bfp.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))
		data := b.Get([]byte(path))
		if data == nil {
			return dp.ErrNotExist
		}
		deserializeFile(file, data)
		return nil
	})
	return file, err
}

func (bfp *Provider) Ls(path string, limit int, offset int) ([]*dp.File, error) {
	path = filepath.Clean(path)
	log.Info().Str("cmd", "ls").Str("path", path).Int("limit", limit).Int("offset", offset).Msg("")
	var files []*dp.File
	err := bfp.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))
		c := b.Cursor()
		prefix := []byte(path)
		var skipped, collected int
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			// Skip the root path itself
			if string(k) == path || !findDirectChild(path, string(k)) {
				continue
			}
			if limit > 0 && collected >= limit {
				break
			}
			if skipped < offset {
				skipped++
				continue
			}
			file := new(dp.File)
			deserializeFile(file, v)
			files = append(files, file)
			collected++
		}
		return nil
	})
	return files, err
}

func (bfp *Provider) Touch(path string) error {
	path = filepath.Clean(path)
	log.Info().Str("cmd", "touch").Str("path", path).Msg("")
	return bfp.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))
		existingFile := b.Get([]byte(path))
		// If the file does not exist, create it
		if existingFile == nil {
			data := serializeFile(dp.File{Name: path, Dir: false, MTime: time.Now()})
			return b.Put([]byte(path), data)
		}
		return nil
	})
}

func (bfp *Provider) Mkdir(path string) error {
	path = filepath.Clean(path)
	log.Info().Str("cmd", "mkdir").Str("path", path).Msg("")
	return bfp.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))
		existingFile := b.Get([]byte(path))
		// Check if the directory already exists
		if existingFile != nil {
			return dp.ErrExist
		}
		data := serializeFile(dp.File{Name: path, Dir: true, MTime: time.Now()})
		return b.Put([]byte(path), data)
	})
}

func (bfp *Provider) Rm(path string) error {
	path = filepath.Clean(path)
	log.Info().Str("cmd", "rm").Str("path", path).Msg("")
	return bfp.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))

		// Check if the directory exists
		if data := b.Get([]byte(path)); data == nil {
			return dp.ErrNotExist
		}
		// Delete the specified directory
		if err := b.Delete([]byte(path)); err != nil {
			return err
		}
		// Delete all children in the directory
		prefix := []byte(path + "/")
		c := b.Cursor()
		file := new(dp.File)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			deserializeFile(file, v)
			if err := b.Delete(k); err != nil {
				return err
			}
			if !file.Dir {
				err := bfp.DeleteNodes(file.Id)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (bfp *Provider) Mv(oldPath, newPath string) error {
	oldPath = filepath.Clean(oldPath)
	newPath = filepath.Clean(newPath)
	file := new(dp.File)
	log.Info().Str("cmd", "mv").Str("new", newPath).Str("old", oldPath).Msg("")
	return bfp.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))
		if exist := b.Get([]byte(newPath)); exist != nil {
			return dp.ErrExist
		}
		// Move the specified file or directory
		data := b.Get([]byte(oldPath))
		if data == nil {
			return dp.ErrNotExist
		}
		deserializeFile(file, data)
		file.Name = newPath
		if err := b.Delete([]byte(oldPath)); err != nil {
			return err
		}
		if err := b.Put([]byte(newPath), serializeFile(*file)); err != nil {
			return err
		}
		// Move all children in the directory
		prefix := []byte(oldPath + "/")
		newPrefix := []byte(newPath + "/")
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			newKey := append(newPrefix, k[len(prefix):]...)
			if err := b.Delete(k); err != nil {
				return err
			}
			deserializeFile(file, v)
			file.Name = string(newKey)
			if err := b.Put(newKey, serializeFile(*file)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (bfp *Provider) CHTime(path string, newMTime time.Time) error {
	path = filepath.Clean(path)
	log.Info().Str("cmd", "chtimes").Str("path", path).Msg("")
	return bfp.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("fs"))
		fileData := b.Get([]byte(path))
		// Check if the file or directory exists
		if fileData == nil {
			return dp.ErrNotExist
		}
		file := new(dp.File)
		// Deserialize the file data
		deserializeFile(file, fileData)
		// Update the modification time
		file.MTime = newMTime
		// Serialize the updated file data
		return b.Put([]byte(path), serializeFile(*file))
	})
}

func (bfp *Provider) Close() error {
	return bfp.db.Close()
}
