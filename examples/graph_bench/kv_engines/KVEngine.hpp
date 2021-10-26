//
// Created by zhanghuigui on 2021/10/18.
//

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <cstdio>

#include <kvdk/status.hpp>
#include <kvdk/engine.hpp>

typedef kvdk::Status Status;

static void SimpleLoger(const std::string& content ) {
	fprintf(stdout,  "[GRAPH-BENCH]%s", content.c_str());
}

// The simple abstract of the different engines.
//
// Any db engine whole want to connect the graph storage bench
// should inherent the class and implementation the function.
class KVEngine {
public:
	class Iterator {
	public:
		virtual void Seek(const std::string& key) = 0;
		virtual void SeekToFirst() = 0;
		virtual void Next() = 0;
		virtual void Prev() = 0;
		virtual bool Valid() = 0;

		virtual std::string Key() = 0;
		virtual std::string Value() = 0;
	};

	virtual Status Get(const std::string& key, std::string* value) = 0;
	virtual Status Put(const std::string& key, const std::string& value) = 0;
	virtual Status Delete(const std::string& key) = 0;
	virtual Iterator* NewIterator() = 0;
};

// KVDK engine
class PMemKVDK : public KVEngine {
public:
	explicit PMemKVDK(const std::string &db_path);
	~PMemKVDK();
	Status Put(const std::string& key, const std::string& value) override;
	Status Get(const std::string& key, std::string* value) override;
	Status Delete(const std::string& key) override;
	KVEngine::Iterator* NewIterator() override;
private:
	kvdk::Engine* db_;
	kvdk::Configs options_;
	std::string path_;
	std::string collection_; // for sorted scan in kvdk
};

// Doesn't implementation
class RocksEngine : public KVEngine {
public:
	explicit RocksEngine(const std::string& db_path) : path_(db_path) {}
	Status Get(const std::string& key, std::string* value) override { return Status::Ok; }
	Status Put(const std::string& key, const std::string& value) override { return Status::Ok; }
	Status Delete(const std::string& key) override { return Status::Ok; }
	Iterator* NewIterator() override { return nullptr; }

private:
	std::string path_;
};
