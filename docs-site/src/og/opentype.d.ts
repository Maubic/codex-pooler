// The part of opentype.js the social cards use.
declare module "opentype.js" {
  type TextOptions = { kerning?: boolean; tracking?: number; letterSpacing?: number };
  export interface Path {
    toPathData(decimalPlaces?: number): string;
  }
  export interface Font {
    unitsPerEm: number;
    ascender: number;
    descender: number;
    getPath(text: string, x: number, y: number, fontSize: number, options?: TextOptions): Path;
    getAdvanceWidth(text: string, fontSize: number, options?: TextOptions): number;
  }
  const opentype: { parse(buffer: ArrayBuffer): Font };
  export default opentype;
}
