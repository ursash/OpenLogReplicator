// Isolated P9 regression for actual Parser BEGIN/COMMIT guards, linked to production objects.
// Input: one JSON case on stdin containing immutable decoded vector/header bytes.
// No Oracle connection, writer, checkpoint, schema import or emitted DATA.
#include <iostream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include "src/common/Ctx.h"
#include "src/common/Thread.h"
#include "src/common/RedoLogRecord.h"
#include "src/common/exception/RuntimeException.h"
#include "src/locales/Locales.h"
#include "src/metadata/Metadata.h"
#include "src/metadata/Schema.h"
#include "src/reader/ReaderFilesystem.h"
#include "src/parser/Transaction.h"
#include "src/parser/TransactionBuffer.h"
#include "src/parser/OpCode0502.h"
#include "src/parser/OpCode0504.h"
// Access only for this test translation unit. Parser.cpp and its compiled guards are unmodified by the fixture.
#define protected public
#include "src/parser/Parser.h"
#undef protected

using namespace OpenLogReplicator;

class FixtureThread final : public Thread {
    void run() override {}
public:
    explicit FixtureThread(Ctx* ctx): Thread(ctx, "commit-scope-fixture") {}
    std::string getName() const override { return "commit-scope-fixture"; }
};

static std::vector<uint8_t> unhex(const std::string& s) {
    if (s.size() % 2 != 0)
        throw std::runtime_error("odd hex size");
    std::vector<uint8_t> result;
    for (size_t i = 0; i < s.size(); i += 2)
        result.push_back(static_cast<uint8_t>(std::stoul(s.substr(i, 2), nullptr, 16)));
    return result;
}

struct InputRecord {
    std::vector<uint8_t> bytes;
    RedoLogRecord rec;
    InputRecord(Ctx& ctx, const rapidjson::Value& value): bytes(unhex(value["vector_hex"].GetString())) {
        const auto header = unhex(value["record_header_hex"].GetString());
        rec.clear();
        rec.dataExt = bytes.data();
        rec.size = static_cast<typeSize>(bytes.size());
        rec.opCode = (static_cast<typeOp1>(bytes[0]) << 8) | bytes[1];
        rec.cls = ctx.read16(bytes.data() + 2);
        rec.usn = static_cast<typeUsn>((rec.cls - 15) / 2);
        rec.afn = static_cast<typeAfn>(ctx.read32(bytes.data() + 4) & 0xffff);
        rec.dba = ctx.read32(bytes.data() + 8);
        rec.scnRecord = ctx.readScn(bytes.data() + 12);
        rec.seq = bytes[20];
        rec.typ = bytes[21];
        rec.conId = ctx.read16(bytes.data() + 24);
        rec.flgRecord = ctx.read16(bytes.data() + 28);
        rec.sequence = Seq(value["sequence"].GetUint());
        rec.thread = value["thread"].GetUint();
        rec.scn = Scn(ctx.read32(header.data() + 8) | (static_cast<uint64_t>(ctx.read16(header.data() + 6)) << 32));
        rec.subScn = ctx.read16(header.data() + 12);
        rec.timestamp = Time(value["lwn_timestamp"].GetUint());
        rec.fileOffset = FileOffset(value["record_file_offset"].GetUint64() + value["vector_record_offset"].GetUint());
        rec.fieldSizesDelta = 32;
        rec.fieldCnt = (ctx.read16(bytes.data() + 32) - 2) / 2;
        rec.fieldPos = 32 + ((ctx.read16(bytes.data() + 32) + 2) & 0xfffc);
        if (rec.opCode == 0x0502)
            OpCode0502::process0502(&ctx, &rec);
        else if (rec.opCode == 0x0504)
            OpCode0504::process0504(&ctx, &rec);
        else
            throw std::runtime_error("fixture input is not BEGIN or COMMIT");
        if (rec.xid.usn() != value["usn"].GetUint() || rec.xid.slt() != value["slt"].GetUint() ||
            rec.xid.sqn() != value["sqn"].GetUint() || rec.conId != value["con_id"].GetUint() ||
            rec.scn.getData() != value["record_scn"].GetUint64() || rec.flg != value["flags"].GetUint() ||
            rec.dbId != value["pdb_id"].GetUint())
            throw std::runtime_error("production opcode decode differs from independent raw-byte decode");
    }
};

int main() {
    try {
        const std::string input((std::istreambuf_iterator<char>(std::cin)), std::istreambuf_iterator<char>());
        rapidjson::Document doc;
        doc.Parse(input.c_str());
        if (doc.HasParseError())
            throw std::runtime_error("invalid case JSON");
        Ctx ctx;
        FixtureThread thread(&ctx);
        ctx.parserThread = &thread;
        ctx.flags = 69728;
        ctx.version = RedoLogRecord::REDO_VERSION_19_0;
        ctx.initialize(16, 32, 4, 4, 0, 4, 4, 4);
        Locales locales;
        Metadata metadata(&ctx, &locales, "PARSER_FIXTURE_ONLY", Scn::none(), Seq::none(), "", 0);
        metadata.dbId = doc["metadata_db_id"].GetUint();
        metadata.conId = doc["metadata_con_id"].GetUint();
        metadata.racOnline = doc["native"].GetBool();
        metadata.firstDataScn = Scn(doc["cut"].GetUint64());
        metadata.firstSchemaScn = Scn(doc["cut"].GetUint64());
        TransactionBuffer buffer(&ctx);
        auto parser = std::make_unique<Parser>(&ctx, nullptr, &metadata, &buffer, 0, "immutable-raw-vector-fixture");
        ReaderFilesystem reader(&ctx, "fixture", "PARSER_FIXTURE_ONLY", 0, false);
        reader.initialize();
        parser->reader = &reader;
        parser->requireBegin = true;
        const std::string scenario(doc["scenario"].GetString());
        InputRecord record(ctx, doc["record"]);
        int code = 0;
        std::string message;
        bool before = false;
        bool began = false;
        std::string phase = "setup";
        try {
            if (scenario == "begin-only") {
                phase = "begin";
                parser->appendToTransactionBegin(&record.rec);
            } else {
                if (doc.HasMember("begin")) {
                    InputRecord begin(ctx, doc["begin"]);
                    phase = "begin";
                    parser->appendToTransactionBegin(&begin.rec);
                }
                if (scenario == "missing-partial") {
                    // Real transaction buffer state as for an observed fragment without its BEGIN; no data/cursor is fabricated.
                    buffer.findTransaction(metadata.schema->xmlCtxDefault, record.rec.xid, record.rec.conId, record.rec.thread, false, true, false);
                }
                Transaction* transaction = buffer.findTransaction(metadata.schema->xmlCtxDefault, record.rec.xid, record.rec.conId,
                                                                  record.rec.thread, true, false, false);
                before = transaction != nullptr;
                began = before && transaction->begin;
                if (began)
                    throw std::runtime_error("commit fixture unexpectedly has a complete BEGIN; no DATA replay intended");
                phase = "commit";
                parser->appendToTransactionCommit(&record.rec);
            }
        } catch (const RuntimeException& ex) {
            code = ex.code;
            message = ex.msg;
        }
        if (scenario == "begin-only") {
            Transaction* transaction = buffer.findTransaction(metadata.schema->xmlCtxDefault, record.rec.xid, record.rec.conId,
                                                              record.rec.thread, true, false, false);
            before = transaction != nullptr;
            began = before && transaction->begin;
        }
        buffer.purge();
        rapidjson::StringBuffer output;
        rapidjson::Writer<rapidjson::StringBuffer> writer(output);
        writer.StartObject();
        writer.Key("observed_code"); writer.Int(code);
        writer.Key("message"); writer.String(message.c_str());
        writer.Key("phase"); writer.String(phase.c_str());
        writer.Key("transaction_present"); writer.Bool(before);
        writer.Key("transaction_begin"); writer.Bool(began);
        writer.Key("production_opcode_decode_matches"); writer.Bool(true);
        writer.Key("record_con_id"); writer.Uint(record.rec.conId);
        writer.Key("record_scn"); writer.Uint64(record.rec.scn.getData());
        writer.Key("xid"); writer.String(record.rec.xid.toString().c_str());
        writer.EndObject();
        std::cout << "SCOPE_RESULT " << output.GetString() << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "FIXTURE_INVALID " << ex.what() << '\n';
        return 2;
    }
}
