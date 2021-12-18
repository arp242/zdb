package sqltoken

import (
	"reflect"
	"testing"
)

var commonCases = []Tokens{
	{},
	{
		{Type: Word, Text: "c01"},
	},
	{
		{Type: Word, Text: "c02"},
		{Type: Semicolon, Text: ";"},
		{Type: Word, Text: "morestuff"},
	},
	{
		{Type: Word, Text: "c03"},
		{Type: Comment, Text: "--cmt;\n"},
		{Type: Word, Text: "stuff2"},
	},
	{
		{Type: Word, Text: "c04"},
		{Type: Punctuation, Text: "-"},
		{Type: Word, Text: "an"},
		{Type: Punctuation, Text: "-"},
		{Type: Word, Text: "dom"},
	},
	{
		{Type: Word, Text: "c05_singles"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "''"},
		{Type: Whitespace, Text: " \t"},
		{Type: Literal, Text: "'\\''"},
		{Type: Semicolon, Text: ";"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "';\\''"},
	},
	{
		{Type: Word, Text: "c06_doubles"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `""`},
		{Type: Whitespace, Text: " \t"},
		{Type: Literal, Text: `"\""`},
		{Type: Semicolon, Text: ";"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `";\""`},
	},
	{
		{Type: Word, Text: "c07_singles"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: "''"},
		{Type: Whitespace, Text: " \t"},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: "'\\''"},
		{Type: Semicolon, Text: ";"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: "';\\''"},
	},
	{
		{Type: Word, Text: "c08_doubles"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: `""`},
		{Type: Whitespace, Text: " \t"},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: `"\""`},
		{Type: Semicolon, Text: ";"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: `";\""`},
	},
	{
		{Type: Word, Text: "c09"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "r"},
		{Type: Punctuation, Text: "-"},
		{Type: Word, Text: "an"},
		{Type: Punctuation, Text: "-"},
		{Type: Word, Text: "dom"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `";;"`},
		{Type: Semicolon, Text: ";"},
		{Type: Literal, Text: "';'"},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: `";"`},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Literal, Text: "';'"},
		{Type: Punctuation, Text: "-"},
	},
	{
		{Type: Word, Text: "c10"},
		{Type: Punctuation, Text: "-//"},
	},
	{
		{Type: Word, Text: "c11"},
		{Type: Punctuation, Text: "-//-/-"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c12"},
		{Type: Punctuation, Text: "/"},
		{Type: Literal, Text: `";"`},
		{Type: Whitespace, Text: "\r\n"},
		{Type: Literal, Text: `";"`},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-/"},
		{Type: Literal, Text: `";"`},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c13"},
		{Type: Punctuation, Text: "/"},
		{Type: Literal, Text: "';'"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "';'"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "/*;*/"},
		{Type: Punctuation, Text: "-/"},
		{Type: Literal, Text: "';'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c14"},
		{Type: Punctuation, Text: "-"},
		{Type: Comment, Text: "/*;*/"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-/"},
		{Type: Comment, Text: "/*\n\t;*/"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c15"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".5"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c16"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".5"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0.5"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "30.5"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40.13"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40.15e8"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40e8"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".4e8"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".4e20"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c17"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "/* foo \n */"},
	},
	{
		{Type: Word, Text: "c18"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "'unterminated "},
	},
	{
		{Type: Word, Text: "c19"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `"unterminated `},
	},
	{
		{Type: Word, Text: "c20"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `'unterminated \`},
	},
	{
		{Type: Word, Text: "c21"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `"unterminated \`},
	},
	{
		{Type: Word, Text: "c22"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ".@"},
	},
	{
		{Type: Word, Text: "c23"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ".@"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c24"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "ee"},
	},
	{
		{Type: Word, Text: "c25"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "eg"},
	},
	{
		{Type: Word, Text: "c26"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "ee"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c27"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "eg"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c28"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "/* foo "},
	},
	{
		{Type: Word, Text: "c29"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7e8"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c30"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7e8"},
	},
	{
		{Type: Word, Text: "c31"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7.0"},
		{Type: Word, Text: "e"},
	},
	{
		{Type: Word, Text: "c32"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7.0"},
		{Type: Word, Text: "e"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c33"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "eèҾ"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "ҾeèҾ"},
	},
	{
		{Type: Word, Text: "c34"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "⁖"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "+⁖"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "+⁖*"},
	},
	{
		{Type: Word, Text: "c35"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒"},
	},
	{
		{Type: Word, Text: "c36"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c37"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒⎖๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c38"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "⎖๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c39"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "-- comment w/o end"},
	},
	{
		{Type: Word, Text: "c40"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c40"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "abnormal"},
		{Type: Whitespace, Text: " "}, // this is a unicode space character
		{Type: Word, Text: "space"},
	},
	{
		{Type: Word, Text: "c41"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "abnormal"},
		{Type: Whitespace, Text: "  "}, // this is a unicode space character
		{Type: Word, Text: "space"},
	},
	{
		{Type: Word, Text: "c42"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "abnormal"},
		{Type: Whitespace, Text: "  "}, // this is a unicode space character
		{Type: Word, Text: "space"},
	},
	{
		{Type: Word, Text: "c43"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "."},
	},
	{
		{Type: Word, Text: "c44"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c45"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3e๒๒๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c46"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7"},
	},
	{
		{Type: Word, Text: "c47"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7e19"},
	},
	{
		{Type: Word, Text: "c48"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7e2"},
	},
	{
		{Type: Word, Text: "c49"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "😀"}, // I'm not sure I agree with the classification
	},
	{
		{Type: Word, Text: "c50"},
		{Type: Whitespace, Text: " \x00"},
	},
	{
		{Type: Word, Text: "c51"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "x"},
		{Type: Whitespace, Text: "\x00"},
	},
	{
		{Type: Comment, Text: "-- c52\n"},
	},
	{
		{Type: Word, Text: "c53"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "z"},
		{Type: Literal, Text: "'not a prefixed literal'"},
	},
	{
		{Type: Word, Text: "c54"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "z"},
	},
}

var mySQLCases = []Tokens{
	{
		{Type: Word, Text: "m01"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Comment, Text: "# /# #;\n"},
		{Type: Whitespace, Text: "\t"},
		{Type: Word, Text: "foo"},
	},
	{
		{Type: Word, Text: "m02"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "'#;'"},
		{Type: Punctuation, Text: ","},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `"#;"`},
		{Type: Punctuation, Text: ","},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Comment, Text: "# /# #;\n"},
		{Type: Whitespace, Text: "\t"},
		{Type: Word, Text: "foo"},
	},
	{
		{Type: Word, Text: "m03"},
		{Type: Whitespace, Text: " "},
		{Type: QuestionMark, Text: "?"},
		{Type: QuestionMark, Text: "?"},
	},
	{
		{Type: Word, Text: "m04"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$"},
		{Type: Number, Text: "5"},
	},
	{
		{Type: Word, Text: "m05"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "U"},
		{Type: Punctuation, Text: "&"},
		{Type: Literal, Text: `'d\0061t\+000061'`},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m06"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0x1f"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "x'1f'"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "X'1f'"},
	},
	{
		{Type: Word, Text: "m07"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0b01"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "b'010'"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "B'110'"},
	},
	{
		{Type: Word, Text: "m08"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0b01"},
	},
	{
		{Type: Word, Text: "m09"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0x01"},
	},
	{
		{Type: Word, Text: "m10"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "x'1f"},
		{Type: Punctuation, Text: "&"},
	},
	{
		{Type: Word, Text: "m10"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "b'1"},
		{Type: Number, Text: "7"},
	},
	{
		{Type: Word, Text: "m11"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$$"},
		{Type: Word, Text: "footext"},
		{Type: Punctuation, Text: "$$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m12"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "b'10"},
	},
	{
		{Type: Word, Text: "m13"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "x'1f"},
	},
	{
		{Type: Word, Text: "m14"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "n'national charset'"},
	},
	{
		{Type: Word, Text: "m14"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "_utf8'redundent'"},
	},
	{
		{Type: Word, Text: "m15"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "=@:$"},
	},
	{
		{Type: Word, Text: "m16"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "n'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m17"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "q"},
		{Type: Literal, Text: "'!martha''s family!'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m18"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "nq"},
		{Type: Literal, Text: "'!martha''s family!'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m19"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "@"},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m20"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "f"},
		{Type: Comment, Text: "#o@o$ "},
	},
	{
		{Type: Word, Text: "m21"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "#foo "},
	},
	{
		{Type: Word, Text: "m22"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m23"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "@"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m24"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "foo"},
		{Type: Comment, Text: "# "},
	},
	{
		{Type: Word, Text: "m25"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "_foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m26"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ":"},
		{Type: Word, Text: "名前"},
		{Type: Punctuation, Text: ")"},
	},
}

var postgreSQLCases = []Tokens{
	{
		{Type: Word, Text: "p01"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "#"},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: "\n"},
	},
	{
		{Type: Word, Text: "p02"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "?"},
		{Type: Whitespace, Text: "\n"},
	},
	{
		{Type: Word, Text: "p03"},
		{Type: Whitespace, Text: " "},
		{Type: DollarNumber, Text: "$17"},
		{Type: DollarNumber, Text: "$8"},
	},
	{
		{Type: Word, Text: "p04"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `U&'d\0061t\+000061'`},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p05"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0"},
		{Type: Word, Text: "x1f"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "x"},
		{Type: Literal, Text: "'1f'"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "X"},
		{Type: Literal, Text: "'1f'"},
	},
	{
		{Type: Word, Text: "p06"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0"},
		{Type: Word, Text: "b01"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "b"},
		{Type: Literal, Text: "'010'"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "B"},
		{Type: Literal, Text: "'110'"},
	},
	{
		{Type: Word, Text: "p07"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$$footext$$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p08"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$$foo!text$$"},
	},
	{
		{Type: Word, Text: "p09"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$q$foo$$text$q$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p10"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$q$foo$$text$q$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p11"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p12"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$$"},
	},
	{
		{Type: Word, Text: "p13"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$"},
		{Type: Word, Text: "q"},
		{Type: Punctuation, Text: "$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p14"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$ҾeèҾ$ $ DLa 32498 $ҾeèҾ$"},
		{Type: Punctuation, Text: "$"},
	},
	{
		{Type: Word, Text: "p15"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$ҾeèҾ$ $ DLa 32498 $ҾeèҾ$"},
	},
	{
		{Type: Word, Text: "p16"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$"},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "-$"},
		{Type: Word, Text: "bar"},
		{Type: Punctuation, Text: "$"},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "-$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p16"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "n"},
		{Type: Literal, Text: "'mysql only'"},
	},
	{
		{Type: Word, Text: "p16"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "_utf8"},
		{Type: Literal, Text: "'mysql only'"},
	},
	{
		{Type: Word, Text: "p17"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "=@:?"},
	},
	{
		{Type: Word, Text: "p18"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "n"},
		{Type: Literal, Text: "'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p19"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "q"},
		{Type: Literal, Text: "'!martha''s family!'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p20"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "nq"},
		{Type: Literal, Text: "'!martha''s family!'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p21"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7E9"},
		{Type: Word, Text: "f"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p22"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7"},
		{Type: Word, Text: "D"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p23"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3"},
		{Type: Word, Text: "d"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p24"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7E11"},
		{Type: Word, Text: "F"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p25"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "@"},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p26"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "f"},
		{Type: Punctuation, Text: "#"},
		{Type: Word, Text: "o"},
		{Type: Punctuation, Text: "@"},
		{Type: Word, Text: "o"},
		{Type: Punctuation, Text: "$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p27"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "#"},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p28"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p29"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "@"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p30"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "#"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p31"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "_foo"},
		{Type: Whitespace, Text: " "},
	},
}

var oracleCases = []Tokens{
	{
		{Type: Word, Text: "o1"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o2"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "n'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o3"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "N'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o4"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "q'!martha's family!'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o5"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "q'<martha's >< family>'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o6"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "Nq'(martha's )( family)'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o7"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "Q'{martha's  family}'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o8"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "nq'[martha's  family]'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o9"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "nq"},
		{Type: Literal, Text: "' martha''s  family '"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o10"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "Q"},
		{Type: Literal, Text: "' martha''s  family '"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o11"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7E9f"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o12"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7D"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o13"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3d"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o14"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7E11F"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o15"},
		{Type: Whitespace, Text: " "},
		{Type: ColonWord, Text: ":foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o16"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ":"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o17"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ":"},
		{Type: Number, Text: "3"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o17"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "@"},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o16"},
		{Type: Whitespace, Text: " "},
		{Type: ColonWord, Text: ":f"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o17"},
		{Type: Whitespace, Text: " "},
		{Type: ColonWord, Text: ":f"},
	},
	{
		{Type: Word, Text: "o18"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "::"},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o19"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ":::"},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "o20"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "::::"},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: " "},
	},
}

var sqlServerCases = []Tokens{
	{
		{Type: Word, Text: "s01"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@foo$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s02"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "f#o@o$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s03"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "#foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s04"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "foo$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s05"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "foo@"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s06"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "foo#"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s07"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "_foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s08"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s09"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "n'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s10"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "N'martha''s family'"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s11"},
		{Type: Whitespace, Text: " "},
		{Type: AtWord, Text: "@foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s12"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "@"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s13"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@8"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s14"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@88"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s15"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@foo$b"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "s16"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@foo$b"},
	},
	{
		{Type: Word, Text: "s17"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@88"},
	},
	{
		{Type: Word, Text: "s18"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@8"},
	},
	{
		{Type: Word, Text: "s19"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "@"},
	},
}

// SQLServer w/o AtWord
var oddball1Cases = []Tokens{
	{
		{Type: Word, Text: "od1"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@foo$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "od2"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@foo"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "od3"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "@"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "od4"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@8"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "od5"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@88"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "od6"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@88"},
	},
	{
		{Type: Word, Text: "od7"},
		{Type: Whitespace, Text: " "},
		{Type: Identifier, Text: "@8"},
	},
	{
		{Type: Word, Text: "od8"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "@"},
	},
}

// SQLx
var oddball2Cases = []Tokens{
	{
		{Type: Word, Text: "sqlx1"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "INSERT"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "INTO"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "("},
		{Type: Word, Text: "a"},
		{Type: Punctuation, Text: ","},
		{Type: Word, Text: "b"},
		{Type: Punctuation, Text: ","},
		{Type: Word, Text: "c"},
		{Type: Punctuation, Text: ","},
		{Type: Word, Text: "d"},
		{Type: Punctuation, Text: ")"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "VALUES"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "("},
		{Type: ColonWord, Text: ":あ"},
		{Type: Punctuation, Text: ","},
		{Type: Whitespace, Text: " "},
		{Type: ColonWord, Text: ":b"},
		{Type: Punctuation, Text: ","},
		{Type: Whitespace, Text: " "},
		{Type: ColonWord, Text: ":キコ"},
		{Type: Punctuation, Text: ","},
		{Type: Whitespace, Text: " "},
		{Type: ColonWord, Text: ":名前"},
		{Type: Punctuation, Text: ")"},
	},
}

func doTests(t *testing.T, config Config, cases ...[]Tokens) {
	for _, tcl := range cases {
		for _, tc := range tcl {
			tc := tc
			desc := "null"
			if len(tc) > 0 {
				desc = tc[0].Text
			}
			t.Run(desc, func(t *testing.T) {
				text := tc.String()
				t.Log("---------------------------------------")
				t.Log(text)
				t.Log("-----------------")
				got := Tokenize(text, config)

				if got.String() != text {
					t.Error(tc.String())
				}

				if !reflect.DeepEqual(tc, got) {
					t.Error(tc.String())
				}
			})
		}
	}
}

func TestMySQLTokenizing(t *testing.T) {
	doTests(t, MySQLConfig(), commonCases, mySQLCases)
}

func TestPostgresSQLTokenizing(t *testing.T) {
	doTests(t, PostgreSQLConfig(), commonCases, postgreSQLCases)
}

func TestOracleTokenizing(t *testing.T) {
	doTests(t, OracleConfig(), commonCases, oracleCases)
}

func TestSQLServerTokenizing(t *testing.T) {
	doTests(t, SQLServerConfig(), commonCases, sqlServerCases)
}

func TestOddbal1Tokenizing(t *testing.T) {
	c := SQLServerConfig()
	c.NoticeAtWord = false
	doTests(t, c, commonCases, oddball1Cases)
}

func TestOddbal2Tokenizing(t *testing.T) {
	c := MySQLConfig()
	c.NoticeColonWord = true
	c.ColonWordIncludesUnicode = true
	doTests(t, c, commonCases, oddball2Cases)
}

func TestStrip(t *testing.T) {
	cases := []struct {
		before string
		after  string
	}{
		{
			before: "",
			after:  "",
		},
		{
			before: "-- stuff\n",
			after:  "",
		},
		{
			before: " /* foo */ bar \n baz  ; ",
			after:  "bar baz",
		},
	}
	for _, tc := range cases {
		t.Run(tc.before, func(t *testing.T) {
			ts := TokenizeMySQL(tc.before)
			have := ts.Strip().String()
			if have != tc.after {
				t.Errorf("\nhave: %v\nwant: %v", have, tc.after)
			}
		})
	}
}

func TestCmdSplit(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{
			input: "",
			want:  []string{},
		},
		{
			input: "-- stuff\n",
			want:  []string{},
		},
		{
			input: " /* foo */ bar \n baz  ; ",
			want:  []string{"bar baz"},
		},
		{
			input: " /* foo */ bar \n ;baz  ; ",
			want:  []string{"bar", "baz"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			ts := TokenizeMySQL(tc.input)

			have := ts.CmdSplit().Strings()
			if !reflect.DeepEqual(have, tc.want) {
				t.Errorf("\nhave: %v\nwant: %v", have, tc.want)

			}
		})
	}
}
