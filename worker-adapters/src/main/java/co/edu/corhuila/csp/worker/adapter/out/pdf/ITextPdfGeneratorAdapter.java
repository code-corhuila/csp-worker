package co.edu.corhuila.csp.worker.adapter.out.pdf;

import co.edu.corhuila.csp.worker.application.port.out.pdf.TicketPdfGeneratorPort;
import co.edu.corhuila.csp.worker.application.usecase.TicketPdfData;
import com.itextpdf.kernel.geom.PageSize;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import com.itextpdf.layout.properties.TextAlignment;
import com.itextpdf.layout.properties.UnitValue;
import com.itextpdf.io.image.ImageDataFactory;
import com.itextpdf.layout.element.Image;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import org.springframework.stereotype.Component;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.List;

@Component
public class ITextPdfGeneratorAdapter implements TicketPdfGeneratorPort {
    @Override
    public byte[] generateTicketPdf(TicketPdfData t) {
        try (var baos = new ByteArrayOutputStream()) {
            var doc = new Document(new com.itextpdf.kernel.pdf.PdfDocument(new com.itextpdf.kernel.pdf.PdfWriter(baos)), PageSize.A6.rotate());
            doc.add(new Paragraph("CineSync Platform").setFontSize(20).setBold().setTextAlignment(com.itextpdf.layout.properties.TextAlignment.CENTER).setMarginBottom(10));
            doc.add(new Paragraph("Digital Ticket").setFontSize(16).setTextAlignment(com.itextpdf.layout.properties.TextAlignment.CENTER).setMarginBottom(20));
            var table = new Table(UnitValue.createPercentArray(new float[]{30, 70})).useAllAvailableWidth();
            addRow(table, "Movie", t.movieTitleSnapshot());
            addRow(table, "Room", t.roomNameSnapshot());
            addRow(table, "Showtime", t.showtimeStartsAt().toString());
            addRow(table, "Seats", String.join(", ", t.seatLabelsSnapshot()));
            addRow(table, "Reservation", t.reservationId().toString());
            addRow(table, "QR", t.qrPayload());
            if (t.concessionsSnapshot() != null && !t.concessionsSnapshot().isEmpty()) {
                addRow(table, "Concessions", t.concessionsSnapshot().stream().map(c -> c.name() + " x" + c.quantity()).reduce((a,b)->a+", "+b).orElse(""));
            }
            doc.add(table);
            doc.add(new Paragraph("\n"));
            var qr = new Image(com.itextpdf.io.image.ImageDataFactory.create(genQR(t.qrPayload()))).setWidth(100).setHeight(100);
            doc.add(qr.setHorizontalAlignment(com.itextpdf.layout.properties.HorizontalAlignment.CENTER));
            doc.add(new Paragraph(t.qrPayload()).setFontSize(8).setTextAlignment(com.itextpdf.layout.properties.TextAlignment.CENTER).setMarginTop(5));
            doc.close();
            return baos.toByteArray();
        } catch (Exception e) { throw new RuntimeException("PDF gen failed", e); }
    }
    private void addRow(Table t, String k, String v) { t.addCell(new com.itextpdf.layout.element.Cell().add(new Paragraph(k).setBold()).setBorder(null)).addCell(new com.itextpdf.layout.element.Cell().add(new Paragraph(v)).setBorder(null)); }
    private byte[] genQR(String s) {
        try { var bm = new QRCodeWriter().encode(s, BarcodeFormat.QR_CODE, 300, 300); var img = MatrixToImageWriter.toBufferedImage(bm); var baos = new java.io.ByteArrayOutputStream(); javax.imageio.ImageIO.write(img, "PNG", baos); return baos.toByteArray(); }
        catch (Exception e) { throw new RuntimeException("QR failed", e); }
    }
}