// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Write operations issued through NewTransactionOnBranch must resolve the set of existing files to
// delete/overwrite/dedupe from the *target branch* head, not from main's head.
//
// Every fixture makes main and "feature" reference different files so that a
// planner reading the wrong branch observes a different file set;
// the assertions would therefore still pass a table with no read-side planning at
// all only if that planning consulted the correct branch.

func newBranchPlanTable(t *testing.T, props iceberg.Properties) *table.Table {
	t.Helper()

	if props == nil {
		props = iceberg.Properties{}
	}
	if _, ok := props[table.PropertyFormatVersion]; !ok {
		props[table.PropertyFormatVersion] = "2"
	}

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "branch_planning"}, meta, metaLoc, fsF, cat)
}

func addParquet(t *testing.T, tbl *table.Table, ref, name, json string) (*table.Table, string) {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	path := tbl.Location() + "/data/" + name
	writeParquetFile(t, path, arrowSc, json)

	tx := tbl.NewTransactionOnBranch(ref)
	require.NoError(t, tx.AddFiles(context.Background(), []string{path}, nil, false))
	tbl, err = tx.Commit(context.Background())
	require.NoError(t, err)

	return tbl, path
}

func appendRows(t *testing.T, tbl *table.Table, ref, json string) *table.Table {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSc, []string{json})
	require.NoError(t, err)
	defer data.Release()

	tx := tbl.NewTransactionOnBranch(ref)
	require.NoError(t, tx.AppendTable(context.Background(), data, 1<<20, nil))
	tbl, err = tx.Commit(context.Background())
	require.NoError(t, err)

	return tbl
}

// newDivergentBranchTable builds a table whose main and feature heads diverge:
//
//	main:    {1,2,3}
//	feature: {1,2,3,100}  (forks from main, then adds a branch-only file)
//
// The id=100 file lives only on the branch, so any planner that reads main's
// head never sees it — which is what makes the branch assertions non-vacuous.
func newDivergentBranchTable(t *testing.T, props iceberg.Properties) *table.Table {
	t.Helper()

	tbl := newBranchPlanTable(t, props)
	tbl, _ = addParquet(t, tbl, table.MainBranch, "main.parquet",
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`)
	tbl, _ = addParquet(t, tbl, "feature", "branch.parquet", `[{"id":100,"data":"z"}]`)

	require.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "fixture main head")
	require.Equal(t, []int64{1, 2, 3, 100}, idsInRef(t, tbl, "feature"), "fixture feature head")

	return tbl
}

func idsInRef(t *testing.T, tbl *table.Table, ref string) []int64 {
	t.Helper()

	scan, err := tbl.Scan().UseRef(ref)
	require.NoError(t, err)

	_, itr, err := scan.ToArrowRecords(context.Background())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		col, ok := rec.Column(idIdx[0]).(*array.Int64)
		require.True(t, ok, "id column must be Int64, got %T", rec.Column(idIdx[0]))
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids
}

func dataFilesInRef(t *testing.T, tbl *table.Table, ref string) []iceberg.DataFile {
	t.Helper()

	scan, err := tbl.Scan().UseRef(ref)
	require.NoError(t, err)
	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)

	files := make([]iceberg.DataFile, 0, len(tasks))
	for _, task := range tasks {
		files = append(files, task.File)
	}

	return files
}

func branchOnlyDataFile(t *testing.T, tbl *table.Table) iceberg.DataFile {
	t.Helper()

	mainPaths := make(map[string]struct{})
	for _, f := range dataFilesInRef(t, tbl, table.MainBranch) {
		mainPaths[f.FilePath()] = struct{}{}
	}
	for _, f := range dataFilesInRef(t, tbl, "feature") {
		if _, ok := mainPaths[f.FilePath()]; !ok {
			return f
		}
	}

	t.Fatal("no branch-only data file found on feature head")

	return nil
}

func realDataFile(t *testing.T, tbl *table.Table, name, json string, recordCount int64) iceberg.DataFile {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	path := tbl.Location() + "/data/" + name
	writeParquetFile(t, path, arrowSc, json)

	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, recordCount, 512)
	require.NoError(t, err)

	return b.Build()
}

func liveDVCountInRef(t *testing.T, tbl *table.Table, ref string) int {
	t.Helper()

	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)

	snap := tbl.SnapshotByName(ref)
	require.NotNil(t, snap)
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	count := 0
	for _, m := range manifests {
		for entry, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			df := entry.DataFile()
			if table.IsDeletionVector(df) && df.ReferencedDataFile() != nil {
				count++
			}
		}
	}

	return count
}

func mainHeadID(t *testing.T, tbl *table.Table) int64 {
	t.Helper()
	snap := tbl.SnapshotByName(table.MainBranch)
	require.NotNil(t, snap)

	return snap.SnapshotID
}

// TestBranchDeletePlansAgainstBranchHead covers classifyFilesForDeletions and
// classifyFilesForFilteredDeletions (copy-on-write Delete). A main-based plan
// never sees the branch-only file, so it would leave id=100 behind.
func TestBranchDeletePlansAgainstBranchHead(t *testing.T) {
	ctx := context.Background()

	t.Run("filtered delete drops the branch-only file", func(t *testing.T) {
		tbl := newDivergentBranchTable(t, nil)
		mainBefore := mainHeadID(t, tbl)

		tx := tbl.NewTransactionOnBranch("feature")
		require.NoError(t, tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(100)), nil))
		tbl, err := tx.Commit(ctx)
		require.NoError(t, err)

		assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, "feature"),
			"the filtered delete must drop the branch-only id=100")
		assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
		assert.Equal(t, mainBefore, mainHeadID(t, tbl), "main head must not move")
	})

	t.Run("full delete clears every branch file", func(t *testing.T) {
		tbl := newDivergentBranchTable(t, nil)
		mainBefore := mainHeadID(t, tbl)

		tx := tbl.NewTransactionOnBranch("feature")
		require.NoError(t, tx.Delete(ctx, iceberg.AlwaysTrue{}, nil))
		tbl, err := tx.Commit(ctx)
		require.NoError(t, err)

		assert.Empty(t, idsInRef(t, tbl, "feature"),
			"an unfiltered delete must remove both branch files; a main-based plan would keep the branch-only file")
		assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
		assert.Equal(t, mainBefore, mainHeadID(t, tbl), "main head must not move")
	})
}

// TestBranchOverwritePlansAgainstBranchHead covers the copy-on-write deletion
// planning behind a full Overwrite. A main-based plan would delete only main's
// file and retain the branch-only id=100 alongside the new data.
func TestBranchOverwritePlansAgainstBranchHead(t *testing.T) {
	ctx := context.Background()
	tbl := newDivergentBranchTable(t, nil)
	mainBefore := mainHeadID(t, tbl)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	newData, err := array.TableFromJSON(memory.DefaultAllocator, arrowSc, []string{`[{"id":200,"data":"new"}]`})
	require.NoError(t, err)
	defer newData.Release()

	tx := tbl.NewTransactionOnBranch("feature")
	require.NoError(t, tx.OverwriteTable(ctx, newData, 1<<20, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{200}, idsInRef(t, tbl, "feature"),
		"a full overwrite must replace every branch file; a main-based plan would retain id=100")
	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
	assert.Equal(t, mainBefore, mainHeadID(t, tbl), "main head must not move")
}

func TestBranchAddFilesChecksBranchHead(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects a file already on the branch", func(t *testing.T) {
		tbl := newDivergentBranchTable(t, nil)
		branchOnly := branchOnlyDataFile(t, tbl)

		tx := tbl.NewTransactionOnBranch("feature")
		err := tx.AddFiles(ctx, []string{branchOnly.FilePath()}, nil, false)
		require.Error(t, err, "re-adding a branch-only file must be caught as a duplicate")
		assert.ErrorContains(t, err, "already referenced")
	})

	t.Run("allows a brand-new file", func(t *testing.T) {
		tbl := newDivergentBranchTable(t, nil)
		tbl, _ = addParquet(t, tbl, "feature", "extra.parquet", `[{"id":300,"data":"n"}]`)

		assert.Equal(t, []int64{1, 2, 3, 100, 300}, idsInRef(t, tbl, "feature"))
		assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
	})
}

// TestBranchAddDataFilesChecksBranchHead covers the AddDataFiles duplicate
// check. Re-adding only the branch-only file is non-vacuous: a main-based check
// never sees that file and would wrongly accept the duplicate.
func TestBranchAddDataFilesChecksBranchHead(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects a data file already on the branch", func(t *testing.T) {
		tbl := newDivergentBranchTable(t, nil)
		branchOnly := branchOnlyDataFile(t, tbl)

		tx := tbl.NewTransactionOnBranch("feature")
		err := tx.AddDataFiles(ctx, []iceberg.DataFile{branchOnly}, nil)
		require.Error(t, err, "re-adding the branch-only data file must be caught as a duplicate")
		assert.ErrorContains(t, err, "already referenced")
	})

	t.Run("allows a brand-new data file", func(t *testing.T) {
		tbl := newDivergentBranchTable(t, nil)
		newDF := realDataFile(t, tbl, "add-dd.parquet", `[{"id":400,"data":"n"}]`, 1)

		tx := tbl.NewTransactionOnBranch("feature")
		require.NoError(t, tx.AddDataFiles(ctx, []iceberg.DataFile{newDF}, nil))
		tbl, err := tx.Commit(ctx)
		require.NoError(t, err)

		assert.Equal(t, []int64{1, 2, 3, 100, 400}, idsInRef(t, tbl, "feature"))
		assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
	})
}

// TestBranchReplaceDataFilesPlansAgainstBranchHead covers ReplaceDataFiles
// membership validation. A main-based plan cannot find the branch-only file and
// fails "cannot delete data files that do not belong to the table".
func TestBranchReplaceDataFilesPlansAgainstBranchHead(t *testing.T) {
	ctx := context.Background()
	tbl := newDivergentBranchTable(t, nil)
	mainBefore := mainHeadID(t, tbl)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	newPath := tbl.Location() + "/data/replace.parquet"
	writeParquetFile(t, newPath, arrowSc, `[{"id":500,"data":"r"}]`)

	branchOnly := branchOnlyDataFile(t, tbl)

	tx := tbl.NewTransactionOnBranch("feature")
	require.NoError(t, tx.ReplaceDataFiles(ctx, []string{branchOnly.FilePath()}, []string{newPath}, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3, 500}, idsInRef(t, tbl, "feature"),
		"the branch-only id=100 must be replaced by id=500")
	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
	assert.Equal(t, mainBefore, mainHeadID(t, tbl), "main head must not move")
}

// TestBranchReplaceDataFilesWithDataFilesPlansAgainstBranchHead covers the
// caller-supplied-DataFile replace variant against a branch head.
func TestBranchReplaceDataFilesWithDataFilesPlansAgainstBranchHead(t *testing.T) {
	ctx := context.Background()
	tbl := newDivergentBranchTable(t, nil)
	mainBefore := mainHeadID(t, tbl)

	branchOnly := branchOnlyDataFile(t, tbl)
	newDF := realDataFile(t, tbl, "replace-dd.parquet", `[{"id":600,"data":"r"}]`, 1)

	tx := tbl.NewTransactionOnBranch("feature")
	require.NoError(t, tx.ReplaceDataFilesWithDataFiles(ctx,
		[]iceberg.DataFile{branchOnly}, []iceberg.DataFile{newDF}, nil))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3, 600}, idsInRef(t, tbl, "feature"),
		"the branch-only id=100 must be replaced by id=600")
	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
	assert.Equal(t, mainBefore, mainHeadID(t, tbl), "main head must not move")
}

// TestBranchReplaceFilesPlansAgainstBranchHead covers the delete-file removal
// path unique to ReplaceFiles (data-file-only replacement delegates to
// ReplaceDataFilesWithDataFiles). A position-delete file added to the branch
// via RowDelta is then removed with ReplaceFiles; a main-based scan never finds
// that delete file and fails "cannot remove delete files ...".
func TestBranchReplaceFilesPlansAgainstBranchHead(t *testing.T) {
	ctx := context.Background()
	tbl := newDivergentBranchTable(t, nil)
	branchOnly := branchOnlyDataFile(t, tbl)

	// Add a branch-only position delete that removes id=100's single row.
	posDelPath := tbl.Location() + "/data/posdel.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 0}]`, branchOnly.FilePath()))
	posDelBuilder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	posDelFile := posDelBuilder.Build()

	tx := tbl.NewTransactionOnBranch("feature")
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(posDelFile)
	require.NoError(t, rd.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, "feature"),
		"the branch position delete must remove id=100")

	// Removing that branch delete file must resurrect id=100 — which requires
	// ReplaceFiles to locate the delete file on the branch head.
	tx = tbl.NewTransactionOnBranch("feature")
	require.NoError(t, tx.ReplaceFiles(ctx, nil, nil, []iceberg.DataFile{posDelFile}, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3, 100}, idsInRef(t, tbl, "feature"),
		"removing the branch delete file must resurrect id=100")
	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
}

// TestBranchMergeOnReadDeletePlansAgainstBranchHead covers collectExistingDVs.
// Two merge-on-read deletes hit the same branch-only file; the second must fold
// into the first's deletion vector. collectExistingDVs must read the branch
// head to find that DV — a main-based lookup misses it (the file is
// branch-only), writes a second live DV (violating one-DV-per-file), and
// resurrects the first delete.
func TestBranchMergeOnReadDeletePlansAgainstBranchHead(t *testing.T) {
	ctx := context.Background()
	tbl := newBranchPlanTable(t, iceberg.Properties{
		table.PropertyFormatVersion: "3",
		table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
	})

	tbl = appendRows(t, tbl, table.MainBranch,
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`)
	tbl = appendRows(t, tbl, "feature", `[{"id":100,"data":"y"},{"id":101,"data":"z"}]`)
	require.Equal(t, []int64{1, 2, 3, 100, 101}, idsInRef(t, tbl, "feature"))
	mainBefore := mainHeadID(t, tbl)

	tx := tbl.NewTransactionOnBranch("feature")
	require.NoError(t, tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(100)), nil))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCountInRef(t, tbl, "feature"), "first branch delete must write exactly one DV")

	tx = tbl.NewTransactionOnBranch("feature")
	require.NoError(t, tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(101)), nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, liveDVCountInRef(t, tbl, "feature"),
		"the two branch deletes must merge into a single DV, not accumulate a second")
	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, "feature"),
		"both branch rows must stay deleted; a main-based DV lookup would resurrect id=100")
	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
	assert.Equal(t, mainBefore, mainHeadID(t, tbl), "main head must not move")
}

// TestBranchRollbackRejectedOffMain enforces a scope boundary: RollbackToSnapshot
// only ever retargets the main ref, so it must fail loudly on a branch
// transaction rather than silently rolling back main,
// while the main path keeps working.
func TestBranchRollbackRejectedOffMain(t *testing.T) {
	ctx := context.Background()

	t.Run("rejected on a branch transaction", func(t *testing.T) {
		tbl := newDivergentBranchTable(t, nil)

		tx := tbl.NewTransactionOnBranch("feature")
		err := tx.RollbackToSnapshot(mainHeadID(t, tbl))
		require.ErrorIs(t, err, table.ErrInvalidOperation,
			"rollback on a branch transaction must be rejected, not silently applied to main")
		assert.ErrorContains(t, err, "main branch")
	})

	t.Run("allowed on main", func(t *testing.T) {
		tbl := newBranchPlanTable(t, nil)
		tbl, _ = addParquet(t, tbl, table.MainBranch, "m1.parquet", `[{"id":1,"data":"a"}]`)
		firstHead := mainHeadID(t, tbl)
		tbl, _ = addParquet(t, tbl, table.MainBranch, "m2.parquet", `[{"id":2,"data":"b"}]`)
		require.Equal(t, []int64{1, 2}, idsInRef(t, tbl, table.MainBranch))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.RollbackToSnapshot(firstHead))
		tbl, err := tx.Commit(ctx)
		require.NoError(t, err)

		assert.Equal(t, []int64{1}, idsInRef(t, tbl, table.MainBranch),
			"rollback must return main to its first snapshot")
	})
}

// TestBranchOnlyOverwriteKeepsOverwriteSemantics pins the branch-aware head
// lookup that decides an overwrite's operation label. When a table's only data
// lives on a branch (main has no head), a main-based lookup returns nil and
// silently downgrades the overwrite to an append — leaving the branch's
// existing rows in place instead of replacing them. It also verifies the
// intended degrade-to-append when there really is nothing to overwrite.
func TestBranchOnlyOverwriteKeepsOverwriteSemantics(t *testing.T) {
	ctx := context.Background()

	t.Run("branch with a head stays an overwrite", func(t *testing.T) {
		// First write goes to feature: the branch gets a head while main has none.
		tbl := appendRows(t, newBranchPlanTable(t, nil), "feature", `[{"id":1,"data":"a"}]`)
		require.Nil(t, tbl.CurrentSnapshot(), "fixture must leave main empty")
		require.NotNil(t, tbl.SnapshotByName("feature"))

		arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
		require.NoError(t, err)
		newData, err := array.TableFromJSON(memory.DefaultAllocator, arrowSc, []string{`[{"id":200,"data":"z"}]`})
		require.NoError(t, err)
		defer newData.Release()

		tx := tbl.NewTransactionOnBranch("feature")
		require.NoError(t, tx.OverwriteTable(ctx, newData, 1<<20, nil))
		tbl, err = tx.Commit(ctx)
		require.NoError(t, err)

		feat := tbl.SnapshotByName("feature")
		require.NotNil(t, feat)
		require.NotNil(t, feat.Summary)
		assert.Equal(t, table.OpOverwrite, feat.Summary.Operation,
			"the branch has rows to overwrite, so the operation must stay an overwrite")
		assert.Equal(t, []int64{200}, idsInRef(t, tbl, "feature"),
			"the branch's existing row must be replaced, not appended to")
		assert.Nil(t, tbl.CurrentSnapshot(), "overwriting a branch must not create a main head")
	})

	t.Run("empty table degrades to append", func(t *testing.T) {
		tbl := newBranchPlanTable(t, nil)

		arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
		require.NoError(t, err)
		newData, err := array.TableFromJSON(memory.DefaultAllocator, arrowSc, []string{`[{"id":1,"data":"a"}]`})
		require.NoError(t, err)
		defer newData.Release()

		tx := tbl.NewTransaction()
		require.NoError(t, tx.OverwriteTable(ctx, newData, 1<<20, nil))
		tbl, err = tx.Commit(ctx)
		require.NoError(t, err)

		snap := tbl.CurrentSnapshot()
		require.NotNil(t, snap)
		require.NotNil(t, snap.Summary)
		assert.Equal(t, table.OpAppend, snap.Summary.Operation,
			"an overwrite with nothing to overwrite must still be recorded as an append")
		assert.Equal(t, []int64{1}, idsInRef(t, tbl, table.MainBranch))
	})
}

// TestBranchOnlyDeletePlansAgainstBranchHead covers Delete on a table whose data
// lives only on a branch (main has no head). A main-based plan sees nothing and
// silently no-ops; the branch delete must actually remove the branch rows.
func TestBranchOnlyDeletePlansAgainstBranchHead(t *testing.T) {
	ctx := context.Background()
	tbl := appendRows(t, newBranchPlanTable(t, nil), "feature",
		`[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	require.Nil(t, tbl.CurrentSnapshot(), "fixture must leave main empty")
	require.NotNil(t, tbl.SnapshotByName("feature"))

	tx := tbl.NewTransactionOnBranch("feature")
	require.NoError(t, tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{2}, idsInRef(t, tbl, "feature"),
		"the branch delete must remove id=1; a main-based plan (main empty) would no-op")
	assert.Nil(t, tbl.CurrentSnapshot(), "deleting on a branch must not create a main head")
}

// TestBranchRowDeltaTargetsBranchHead covers RowDelta on a divergent branch: the
// delete file it stages must land on and apply to the branch head (RowDelta
// parents through the fast-append producer, which resolves the branch head), leaving main untouched.
func TestBranchRowDeltaTargetsBranchHead(t *testing.T) {
	ctx := context.Background()
	tbl := newDivergentBranchTable(t, nil)
	mainBefore := mainHeadID(t, tbl)
	branchOnly := branchOnlyDataFile(t, tbl)

	// A position delete removing the branch-only file's single row (id=100).
	posDelPath := tbl.Location() + "/data/rd-posdel.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 0}]`, branchOnly.FilePath()))
	posDelBuilder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx := tbl.NewTransactionOnBranch("feature")
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(posDelBuilder.Build())
	require.NoError(t, rd.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, "feature"),
		"the RowDelta must apply on the branch head and remove the branch-only id=100")
	assert.Equal(t, []int64{1, 2, 3}, idsInRef(t, tbl, table.MainBranch), "main data must be untouched")
	assert.Equal(t, mainBefore, mainHeadID(t, tbl), "main head must not move")
}

// TestExplicitMainBranchCommitMatchesDefault verifies the explicit-main path:
// A transaction opened via NewTransactionOnBranch(MainBranch) behaves exactly like
// the default NewTransaction() — it advances main's head with correct parentage and creates no stray ref.
func TestExplicitMainBranchCommitMatchesDefault(t *testing.T) {
	ctx := context.Background()
	tbl := newBranchPlanTable(t, nil)
	tbl, _ = addParquet(t, tbl, table.MainBranch, "m1.parquet", `[{"id":1,"data":"a"}]`)
	firstHead := mainHeadID(t, tbl)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSc, []string{`[{"id":2,"data":"b"}]`})
	require.NoError(t, err)
	defer data.Release()

	tx := tbl.NewTransactionOnBranch(table.MainBranch)
	require.NoError(t, tx.AppendTable(ctx, data, 1<<20, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2}, idsInRef(t, tbl, table.MainBranch),
		"an explicit-main append must advance main")

	head := tbl.SnapshotByName(table.MainBranch)
	require.NotNil(t, head)
	require.NotNil(t, head.ParentSnapshotID)
	assert.Equal(t, firstHead, *head.ParentSnapshotID,
		"the explicit-main snapshot must be parented on main's previous head")

	refCount, hasMain := 0, false
	for name := range tbl.Metadata().Refs() {
		refCount++
		if name == table.MainBranch {
			hasMain = true
		}
	}
	assert.Equal(t, 1, refCount, "an explicit-main commit must not create any extra ref")
	assert.True(t, hasMain, "main ref must be present")
}
