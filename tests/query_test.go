package tests

import (
	"context"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgx/v5"
)

type VerySimpleStruct struct {
	ID   int64  `json:"id" db:"id"`
	Name string `json:"name" db:"name"`
}

type SimpleStructWithSlice struct {
	ID    int64    `json:"id" db:"id"`
	Name  string   `json:"name" db:"name"`
	TxIDs []string `json:"tx_ids" db:"tx_ids"`
}

type SimpleStruct struct {
	ID       int64   `json:"id" db:"id"`
	Name     string  `json:"name" db:"name"`
	Nullable *string `json:"nullable,omitempty" db:"nullable"`
	Boolean  bool    `json:"boolean" db:"boolean"`
}

type ComplexStruct struct {
	ID           uuid.UUID      `json:"id" db:"id"`
	Email        string         `json:"email" db:"email"`
	SimpleStruct SimpleStruct   `json:"simple_struct" db:"simple_struct"`
	Slice        []SimpleStruct `json:"slice" db:"slice"`
	Numbers      []int          `json:"numbers" db:"numbers"`
}

func (s *TestSuite) Test_Query() {
	tests := []struct {
		name   string
		query  string
		result any
		f      func(rows pgx.Rows, ttRes any) (any, error)
	}{
		{
			name:   "one number",
			query:  `select 8`,
			result: int64(8),
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectOneRow[int64](rows, pgx.RowTo[int64])
			},
		},
		{
			name:   "string slice",
			query:  `select x::text from generate_series(1, 10) x`,
			result: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectRows[string](rows, pgx.RowTo[string])
			},
		},
		{
			name:  "slice of addrs structs",
			query: `select x as id, x::text as name from generate_series(1, 3) x`,
			result: []*VerySimpleStruct{
				{ID: 1, Name: "1"},
				{ID: 2, Name: "2"},
				{ID: 3, Name: "3"},
			},
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectRows[*VerySimpleStruct](rows, pgx.RowToAddrOfStructByName[VerySimpleStruct])
			},
		},
		{
			name:  "slice of map structs",
			query: `select x::bigint as id, x::text as name from generate_series(1, 3) x`,
			result: []map[string]any{
				{"id": int64(1), "name": "1"},
				{"id": int64(2), "name": "2"},
				{"id": int64(3), "name": "3"},
			},
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectRows(rows, pgx.RowToMap)
			},
		},
		{
			name:  "one row",
			query: `select 1 as id, 'name' as name, null as nullable, true as boolean`,
			result: SimpleStruct{
				ID:       1,
				Name:     "name",
				Nullable: nil,
				Boolean:  true,
			},
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectOneRow[SimpleStruct](rows, pgx.RowToStructByName[SimpleStruct])
			},
		},
		{
			name: "flat struct slice",
			query: `select x as id, x::text as name, case when (x % 2)::bool then 'qwerty' end as nullable, (x % 2)::bool as boolean
					from generate_series(1, 5) x`,
			result: []SimpleStruct{
				{
					ID:       1,
					Name:     "1",
					Nullable: &[]string{"qwerty"}[0],
					Boolean:  true,
				},
				{
					ID:       2,
					Name:     "2",
					Nullable: nil,
					Boolean:  false,
				},
				{
					ID:       3,
					Name:     "3",
					Nullable: &[]string{"qwerty"}[0],
					Boolean:  true,
				},
				{
					ID:       4,
					Name:     "4",
					Nullable: nil,
					Boolean:  false,
				},
				{
					ID:       5,
					Name:     "5",
					Nullable: &[]string{"qwerty"}[0],
					Boolean:  true,
				},
			},
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectRows[SimpleStruct](rows, pgx.RowToStructByName[SimpleStruct])
			},
		},
		{
			name: "complex struct slice",
			query: `
select ('00000000-0000-0000-0000-00000000000' || x::text)::uuid as id,
       x::text || '@mail.com'                                   as email,
       json_build_object('id', x, 'name', x::text, 'nullable', case when (x % 2)::bool then 'qwerty' end, 'boolean',
                         (x % 2)::bool)                         as simple_struct,
       (select json_agg(json_build_object('id', x, 'name', x::text, 'nullable',
                                          case when (x % 2)::bool then 'qwerty' end, 'boolean', (x % 2)::bool))
        from generate_series(1, 2) x)                           as slice,
       (select array_agg(x) from generate_series(x, 7) x)       as numbers
from generate_series(1, 4) x`,
			result: []ComplexStruct{
				{
					ID:    uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000001")),
					Email: "1@mail.com",
					SimpleStruct: SimpleStruct{
						ID:       1,
						Name:     "1",
						Nullable: &[]string{"qwerty"}[0],
						Boolean:  true,
					},
					Slice: []SimpleStruct{
						{
							ID:       1,
							Name:     "1",
							Nullable: &[]string{"qwerty"}[0],
							Boolean:  true,
						},
						{
							ID:       2,
							Name:     "2",
							Nullable: nil,
							Boolean:  false,
						},
					},
					Numbers: []int{1, 2, 3, 4, 5, 6, 7},
				},
				{
					ID:    uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000002")),
					Email: "2@mail.com",
					SimpleStruct: SimpleStruct{
						ID:       2,
						Name:     "2",
						Nullable: nil,
						Boolean:  false,
					},
					Slice: []SimpleStruct{
						{
							ID:       1,
							Name:     "1",
							Nullable: &[]string{"qwerty"}[0],
							Boolean:  true,
						},
						{
							ID:       2,
							Name:     "2",
							Nullable: nil,
							Boolean:  false,
						},
					},
					Numbers: []int{2, 3, 4, 5, 6, 7},
				},
				{
					ID:    uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000003")),
					Email: "3@mail.com",
					SimpleStruct: SimpleStruct{
						ID:       3,
						Name:     "3",
						Nullable: &[]string{"qwerty"}[0],
						Boolean:  true,
					},
					Slice: []SimpleStruct{
						{
							ID:       1,
							Name:     "1",
							Nullable: &[]string{"qwerty"}[0],
							Boolean:  true,
						},
						{
							ID:       2,
							Name:     "2",
							Nullable: nil,
							Boolean:  false,
						},
					},

					Numbers: []int{3, 4, 5, 6, 7},
				},
				{
					ID:    uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000004")),
					Email: "4@mail.com",
					SimpleStruct: SimpleStruct{
						ID:       4,
						Name:     "4",
						Nullable: nil,
						Boolean:  false,
					},
					Slice: []SimpleStruct{
						{
							ID:       1,
							Name:     "1",
							Nullable: &[]string{"qwerty"}[0],
							Boolean:  true,
						},
						{
							ID:       2,
							Name:     "2",
							Nullable: nil,
							Boolean:  false,
						},
					},

					Numbers: []int{4, 5, 6, 7},
				},
			},
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectRows[ComplexStruct](rows, pgx.RowToStructByPos[ComplexStruct])
			},
		},
		{
			name: "slice of structs with slice",
			query: `
select x as id, x::text as name, (select array_agg(t::text) from generate_series(0, x) t) as tx_ids
from generate_series(-1, 3) x
union all
select 1984 as id, 'empty_array' as name, '{}'::text[] as tx_ids
`,
			result: []SimpleStructWithSlice{
				{
					ID:    -1,
					Name:  "-1",
					TxIDs: nil,
				},
				{
					ID:    0,
					Name:  "0",
					TxIDs: []string{"0"},
				},
				{
					ID:    1,
					Name:  "1",
					TxIDs: []string{"0", "1"},
				},
				{
					ID:    2,
					Name:  "2",
					TxIDs: []string{"0", "1", "2"},
				},
				{
					ID:    3,
					Name:  "3",
					TxIDs: []string{"0", "1", "2", "3"},
				},
				{
					ID:    1984,
					Name:  "empty_array",
					TxIDs: []string{},
				},
			},
			f: func(rows pgx.Rows, ttRes any) (any, error) {
				return pgx.CollectRows[SimpleStructWithSlice](rows, pgx.RowToStructByName[SimpleStructWithSlice])
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			rows, err := s.pgxPool.Query(context.Background(), tt.query)
			s.Require().NoError(err)
			result, err := tt.f(rows, tt.result)
			s.Require().NoError(err)
			s.Equal(tt.result, result)
		})
	}

}
