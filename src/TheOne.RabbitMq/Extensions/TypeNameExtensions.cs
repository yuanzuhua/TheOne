using System;
using System.Text;

namespace TheOne.RabbitMq.Extensions {

    internal static class TypeNameExtensions {
        public static string ExpandTypeName(this Type type) {
            if (type.IsGenericType) {
                return GetGenericTypeName(type);
            }

            return GetTypeName(type);
        }

        private static string GetTypeName(Type type) {
            // Need to expand Arrays of Generic Types like Nullable<Byte>[]
            if (type.IsArray) {
                return type.GetElementType().ExpandTypeName() + "[]";
            }

            var fullname = type.FullName;
            var genericPrefixIndex = type.IsGenericParameter ? 1 : 0;

            if (fullname == null) {
                return genericPrefixIndex > 0 ? "'" + type.Name : type.Name;
            }

            var startIndex = type.Namespace?.Length + 1 ?? 0;  // trim namespace + "."
            var endIndex = fullname.IndexOf("[[", startIndex); // Generic Fullname
            if (endIndex == -1) {
                endIndex = fullname.Length;
            }

            var op = new char[endIndex - startIndex + genericPrefixIndex];

            for (var i = startIndex; i < endIndex; i++) {
                var cur = fullname[i];
                op[i - startIndex + genericPrefixIndex] = cur != '+' ? cur : '.';
            }

            if (genericPrefixIndex > 0) {
                op[0] = '\'';
            }

            return new string(op);
        }

        private static string GetGenericTypeName(Type type) {
            var strVal = type.Name;
            var pos = strVal.IndexOf('`');
            var nameOnly = pos == -1 ? strVal : strVal.Substring(0, pos);

            var sb = new StringBuilder();
            foreach (Type arg in type.GetGenericArguments()) {
                if (sb.Length > 0) {
                    sb.Append(",");
                }

                sb.Append(arg.ExpandTypeName());
            }

            var fullName = $"{nameOnly}<{sb}>";
            return fullName;
        }
    }
}
