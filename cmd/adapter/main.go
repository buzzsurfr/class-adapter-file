package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"

	"github.com/dgraph-io/badger"
	pb "github.com/virtual-class-tutor/class-adapter-file/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	port  = ":50051"
	delim = "."
)

type server struct {
	pb.UnimplementedAdapterServer
	db *badger.DB
}

func (s *server) List(ctx context.Context, in *pb.ListRequest) (*pb.Classes, error) {
	log.Print("List called")
	cs := &pb.Classes{}
	cs.Classes = make([]*pb.Class, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions

		it := txn.NewIterator(opts)
		defer it.Close()

		classMap := make(map[string]int)

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())
			// Split ID from parameter
			lastIndex := strings.LastIndex(k, ".")
			id := k[:lastIndex]
			param := k[lastIndex+1:]

			// Determine if we've already found this Class, add a new placeholder if not
			var index int
			var ok bool
			if index, ok = classMap[id]; !ok {
				index = len(cs.Classes)
				classMap[id] = index
				cs.Classes = append(cs.Classes, &pb.Class{
					Id: id,
				})

			}

			err := item.Value(func(v []byte) error {
				if param == "Name" {
					cs.Classes[index].Name = string(v)
				} else if param == "Semester" {
					cs.Classes[index].Semester = string(v)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.Printf("Error listing from class database: %s", err)
	}
	return cs, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.Class, error) {
	log.Printf("Get called for Id %s", in.Id)
	c := &pb.Class{
		Id:       in.Id,
		Name:     "",
		Semester: "",
	}
	err := s.db.View(func(txn *badger.Txn) error {
		n, nameErr := txn.Get([]byte(in.Id + delim + "Name"))
		if nameErr != nil {
			return nameErr
		}
		nameErr = n.Value(func(val []byte) error {
			c.Name = string(val)
			return nil
		})
		if nameErr != nil {
			return nameErr
		}

		s, semesterErr := txn.Get([]byte(in.Id + delim + "Semester"))
		if semesterErr != nil {
			return semesterErr
		}
		semesterErr = s.Value(func(val []byte) error {
			c.Semester = string(val)
			return nil
		})
		if semesterErr != nil {
			return semesterErr
		}

		return nil
	})
	if err != nil {
		log.Printf("Error reading %s from class database: %s", in.Name, err)
	}
	return c, nil
}

func (s *server) Create(ctx context.Context, in *pb.Class) (*pb.Class, error) {
	log.Printf("Create called for Id %s", in.Id)
	err := s.db.Update(func(txn *badger.Txn) error {
		// Store name
		err := txn.Set([]byte(in.Id+delim+"Name"), []byte(in.Name))
		if err != nil {
			return fmt.Errorf("put %s%sName: %s", in.Id, delim, err)
		}

		// Store semester
		err = txn.Set([]byte(in.Id+delim+"Semester"), []byte(in.Semester))
		if err != nil {
			return fmt.Errorf("put %s%sSemester: %s", in.Id, delim, err)
		}

		return nil
	})
	if err != nil {
		log.Printf("Error saving %s to class database: %s", in.Name, err)
	}

	log.Printf("Added %s to class database", in.Name)
	return in, nil
}

func (s *server) Update(ctx context.Context, in *pb.Class) (*pb.Class, error) {
	log.Printf("Update called for Id %s", in.Id)
	err := s.db.Update(func(txn *badger.Txn) error {
		// Store name
		err := txn.Set([]byte(in.Id+delim+"Name"), []byte(in.Name))
		if err != nil {
			return fmt.Errorf("put %s%sName: %s", in.Id, delim, err)
		}

		// Store semester
		err = txn.Set([]byte(in.Id+delim+"Semester"), []byte(in.Semester))
		if err != nil {
			return fmt.Errorf("put %s%sSemester: %s", in.Id, delim, err)
		}

		return nil
	})
	if err != nil {
		log.Printf("Error saving %s to class database: %s", in.Name, err)
	}

	log.Printf("Added %s to class database", in.Name)
	return in, nil
}

func (s *server) Delete(ctx context.Context, in *pb.Class) (*pb.Empty, error) {
	log.Printf("Delete called for Id %s", in.Id)
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(in.Id + ".Name"))
		if err != nil {
			return fmt.Errorf("delete %s.Name: %s", in.Id, err)
		}
		err = txn.Delete([]byte(in.Id + ".Semester"))
		if err != nil {
			return fmt.Errorf("delete %s.Semester: %s", in.Id, err)
		}
		return nil
	})
	if err != nil {
		log.Printf("Error saving %s to class database: %s", in.Name, err)
	}

	return &pb.Empty{}, nil
}

func main() {
	log.Printf("Opening database...\n")
	dir, err := ioutil.TempDir("", "class")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	log.Printf("Listening on port %v...\n", port)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAdapterServer(s, &server{
		db: db,
	})
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	reflection.Register(s)

	log.Printf("Serving gRPC...\n")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
