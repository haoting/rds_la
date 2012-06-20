/*
  Erlang NIF interface to the rds_la_log analyzer (Erlang/OTP R14 version)
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#define MAX_PARSE_LEN		1200

#define is_space(c) ((c == '\t') || (c == '\n') || (c == '\v') || (c == '\f') || (c == '\r') || (c == ' '))

#define KEEP				"keep"
#define REPLACE				"replace"
#define ERR_UNKNOWN			"unknown"
#define ERR_TOO_BIG			"too_big"
#define ERR_ONLY_SPACE		"only_space"
#define ERR_INVALID_SYNTAX	"invalid_syntax"

#define DEFINE_KEYWORD(KW)											\
			static const unsigned char KW##_key [] = #KW;						\
			static const size_t KW##_len = sizeof(KW##_key) - 1;
#define DEFINE_KEYWORD2(KW, KWC)									\
			static const unsigned char KW##_key [] = KWC;						\
			static const size_t KW##_len = sizeof(KW##_key) - 1;

#define DEFINE_KEYCHAR(KC, CHAR)									\
			static const unsigned char KC##_char = CHAR;

DEFINE_KEYWORD(create)
DEFINE_KEYWORD(alter)
DEFINE_KEYWORD(drop)

// for char in ct islower
static int strnequal_caseless(const unsigned char *cs, const unsigned char *ct, size_t n)
{
	size_t i;
	for (i = 0; (isalpha(cs[i]) && (tolower(cs[i]) == ct[i])); i++) ;

	return 0 == (n - i);
}

static inline int charequal_caseless(const unsigned char cs, const unsigned char ct)
{
	return isalpha(cs) && (tolower(cs) == ct);
}

#define CHECK_KW_QUICK(buf, prefix, kw, kw_len)						\
do																	\
{																	\
	if(charequal_caseless(*buf, prefix))							\
	{																\
		if(strnequal_caseless(buf, kw, kw_len))						\
			return 1;												\
		return 0;													\
	}																\
}																	\
while (0)

static int is_keep(unsigned char *buf, size_t len)
{
	/*
		there 3 key words must keep:
		1. create
		2. alter
		3. drop
	*/
	size_t max_keep_kw_len = create_len;
	if (len < max_keep_kw_len)
		return -1;

	//printf("buf[%d]:%s\n", len, buf);

	CHECK_KW_QUICK(buf, 'c', create_key, create_len);
	CHECK_KW_QUICK(buf, 'a', alter_key, alter_len);
	CHECK_KW_QUICK(buf, 'd', drop_key, drop_len);

	return 0;
}

#define REPLACE_SPACE				' '
#define REPLACE_STRING				'?'
#define REPLACE_DIGITAL				'?'

#define INTO_DST(c)					\
do									\
{									\
	*dst = c;						\
	dst++;							\
}									\
while (0)

#define MOVE_ON						\
do									\
{									\
	if (tmp >= last)				\
		goto replace_digital;		\
	tmp++;							\
} while (0)

//default src's len <= dst's len
static size_t handle_replace_log(unsigned char *src, unsigned char *dst, size_t len)
{
	unsigned char *tmp;
	unsigned char *last = src + len;
	unsigned char *dstart = dst;
	for (tmp = src; tmp < last; )
	{
		//printf("input char: %c\n", *tmp);
		if (isalpha(*tmp)) {
			// keep keyword or identifier
			do
			{
				INTO_DST(*tmp);
				tmp++;
			}
			while ((tmp < last) && (isalnum(*tmp) || (*tmp == '_')));
		} else if ((*tmp == '"') || (*tmp == '\'')) {
			// replace string
			unsigned char quota = *tmp;
			tmp++;
			while (tmp < last)
			{
				if (*tmp == quota)
				{
					tmp++;
					break;
				} else if (*tmp == '\\') {
					tmp++;
				}
				tmp++;
			}
			INTO_DST(REPLACE_STRING);
		} else if (isdigit(*tmp)) {
			// replace digital
			if (*tmp == '0') {
				if (tmp >= last)
				{
				}
				MOVE_ON;
			} else if (*tmp >= '1' && *tmp <= '9') {
				do {
					MOVE_ON;
				} while (*tmp >= '0' && *tmp <= '9');
			} else {
				return 0;
			}
			if (*tmp == '.')
			{
				MOVE_ON;
				while (*tmp >= '0' && *tmp <= '9') {
					MOVE_ON;
				}
			}
			if (*tmp == 'e' || *tmp == 'E') {
				MOVE_ON;

				/* optional sign */
				if (*tmp == '+' || *tmp == '-')
				{
					MOVE_ON;
				}

				if (*tmp >= '0' && *tmp <= '9') {
					do {
						MOVE_ON;
					} while (*tmp >= '0' && *tmp <= '9');
				}
			}
replace_digital:
			INTO_DST(REPLACE_DIGITAL);
		} else if (is_space(*tmp)) {
			// replace space
			do
				tmp++;
			while ((tmp < last) && is_space(*tmp));
			INTO_DST(REPLACE_SPACE);
		} else {
			// keep other symbol
			INTO_DST(*tmp);
			tmp++;
		}
	}
	return dst - dstart;
}

#define ERTS
#ifdef ERTS

#include <erl_nif.h>

#define set_keep(env) set_ok_result(env, enif_make_atom(env, KEEP))
/* replace is ErlNifBinary */
#define set_replace(env, replace) set_ok_result(env,			\
									enif_make_tuple2(env,		\
										enif_make_atom(env, REPLACE), enif_make_binary(env, replace)))
/* error is char * */
#define set_error(env, error) set_error_result(env,				\
									enif_make_atom(env, error))

static inline ERL_NIF_TERM set_ok_result(ErlNifEnv *env, ERL_NIF_TERM result)
{
	return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
}

static inline ERL_NIF_TERM set_error_result(ErlNifEnv *env, ERL_NIF_TERM error)
{
	return enif_make_tuple2(env, enif_make_atom(env, "error"), error);
}

static ERL_NIF_TERM handle_analyze_log(ErlNifEnv *env, unsigned char *buf, size_t len)
{
	unsigned char tbuf[MAX_PARSE_LEN];
	unsigned char *tmp = buf;
	size_t tlen = len;
	// jump white space
	if (len > MAX_PARSE_LEN)
		return set_error(env, ERR_TOO_BIG);

	while (tlen > 0 && is_space(*tmp))
	{
		tmp ++;
		tlen --;
	}
	if (tlen <= 0)
		return set_error(env, ERR_ONLY_SPACE);

	if(is_keep(tmp, tlen) > 0)
		return set_keep(env);

	size_t rlen = handle_replace_log(tmp, tbuf, tlen);

	if (rlen > 0)
	{
		ErlNifBinary replaced;
		enif_alloc_binary(rlen, &replaced);
		memcpy((unsigned char *)replaced.data, (unsigned char *)tbuf, rlen);
		return set_replace(env, &replaced);
	}
	else
		return set_error(env, ERR_INVALID_SYNTAX);
}

/*
	arg: Query
	type: binary
	len: less than MAX_PARSE_LEN
*/
static ERL_NIF_TERM analyze_log_1(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary bin;

	if (argc != 1 || !enif_is_binary(env, argv[0]))
		return enif_make_badarg(env);

	if (!enif_inspect_binary(env, argv[0], &bin))
		return enif_make_badarg(env);

	return handle_analyze_log(env, bin.data, bin.size);
}

static ErlNifFunc analyze_log_NIFs[] = {
	{"analyze_log", 1, &analyze_log_1}
};

ERL_NIF_INIT(rds_la_analyze, analyze_log_NIFs, NULL, NULL, NULL, NULL);

#else

#define INTERNAL_KEEP						1
#define INTERNAL_REPLACE					2
#define INTERNAL_ERROR_UNKNOWN				11
#define INTERNAL_ERROR_TOOBIG				12
#define INTERNAL_ERROR_ONLY_SPACE			13
#define INTERNAL_ERROR_INVALID_SYNTAX		14

static int handle_analyze_log_internal(unsigned char *buf, size_t len, unsigned char** replaced)
{
	const char *err = NULL;
	unsigned char tbuf[MAX_PARSE_LEN];
	unsigned char *tmp = buf;
	size_t tlen = len;

	// jump white space
	if (len > MAX_PARSE_LEN)
		return INTERNAL_ERROR_TOOBIG;

	printf("checked length\n");

	while (tlen > 0 && is_space(*tmp))
	{
		tmp ++;
		tlen --;
	}
	if (tlen <= 0)
		return INTERNAL_ERROR_ONLY_SPACE;

	printf("jumped space\n");

	if(is_keep(tmp, tlen) > 0)
		return INTERNAL_KEEP;

	printf("checked keep\n");

	size_t rlen = handle_replace_log(tmp, tbuf, tlen);
	printf("rlen: %d\n", rlen);
	printf("checked replace\n");

	if (rlen > 0)
	{
		*replaced = (unsigned char *)malloc(rlen);
		memcpy((unsigned char *)*replaced, (unsigned char *)tbuf, rlen);
		return INTERNAL_REPLACE;
	}
	else
		return INTERNAL_ERROR_INVALID_SYNTAX;
}

static void test_analyze_log(unsigned char *buf, size_t len)
{
	unsigned char *replaced = NULL;
	printf("analyze buf[%d]: %s\n", len, buf);
	int res = handle_analyze_log_internal(buf, len, &replaced);
	printf("res: %d\n", res);
	switch (res)
	{
	case INTERNAL_KEEP:
		printf("%s: %s\n", KEEP, buf);
		break;
	case INTERNAL_REPLACE:
		printf("%s: %s\n", REPLACE, replaced);
		break;
	case INTERNAL_ERROR_UNKNOWN:
		printf("error: %s\n", ERR_UNKNOWN);
		break;
	case INTERNAL_ERROR_TOOBIG:
		printf("error: %s\n", ERR_TOO_BIG);
		break;
	case INTERNAL_ERROR_ONLY_SPACE:
		printf("error: %s\n", ERR_ONLY_SPACE);
		break;
	case INTERNAL_ERROR_INVALID_SYNTAX:
		printf("error: %s\n", ERR_INVALID_SYNTAX);
		break;
	}
	free(replaced);
}

int main ()
{
	char sbuf[] = "select f1, f2 from t1 where f1=\"abcd\", f2='b', f3=113";
	size_t sbuf_len = sizeof(sbuf) - 1;
	test_analyze_log(sbuf, sbuf_len);
	char cbuf[] = "create table t";
	size_t cbuf_len = sizeof(cbuf) - 1;
	test_analyze_log(cbuf, cbuf_len);
	char ebuf[] = "a        a";
	size_t ebuf_len = sizeof(ebuf) - 1;
	test_analyze_log(ebuf, ebuf_len);
	return 0;
}
#endif
